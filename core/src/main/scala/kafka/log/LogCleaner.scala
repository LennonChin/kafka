/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.{DataOutputStream, File}
import java.nio._
import java.util.Date
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.message._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._

import scala.collection._

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * 
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy 
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log. 
 * 
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping. 
 * 
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a 
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * 
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * 
 * Cleaned segments are swapped into the log as they become available.
 * 
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * 
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner. 
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * 
 * @param config Configuration parameters for the cleaner Cleaner的配置
 * @param logDirs The directories where offset checkpoints reside 数据目录集合
 * @param logs The pool of logs 主题分区到Log对象的映射字典
 * @param time A way to control the passage of time 当前时间
 */
class LogCleaner(val config: CleanerConfig,
                 val logDirs: Array[File],
                 val logs: Pool[TopicAndPartition, Log], 
                 time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  
  /**
    * for managing the state of partitions being cleaned. package-private to allow access in tests
    * 负责每个Log的压缩状态管理以及cleaner checkpoint信息维护和更新
    * */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond, 
                                        checkIntervalMs = 300, 
                                        throttleDown = true, 
                                        "cleaner-io",
                                        "bytes",
                                        time = time)
  
  /**
    * the threads
    * 用于管理CleanerThread线程
    * */
  private val cleaners = (0 until config.numThreads).map(new CleanerThread(_))
  
  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent", 
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
           })
  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent", 
           new Gauge[Int] {
             def value: Int = {
               val stats = cleaners.map(_.lastStats)
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
               (100 * recopyRate).toInt
             }
           })
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
           })
  
  /**
   * Start the background cleaning
    * 启动cleaner线程
   */
  def startup() {
    info("Starting the log cleaner")
    // 遍历调用start()方法
    cleaners.foreach(_.start())
  }
  
  /**
   * Stop the background cleaning
    * 停止cleaner线程
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    // 遍历调用shutdown()方法
    cleaners.foreach(_.shutdown())
  }
  
  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortCleaning(topicAndPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicAndPartition: TopicAndPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicAndPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortAndPauseCleaning(topicAndPartition)
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.resumeCleaning(topicAndPartition)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topic The Topic to be cleaned
   * @param part The partition of the topic to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topic: String, part: Int, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(TopicAndPartition(topic, part)).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }
  
  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
    * 执行压缩操作的线程
   */
  private class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {
    
    override val loggerName = classOf[LogCleaner].getName
    
    if(config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    // 初始化压缩器
    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt,
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)
    
    @volatile var lastStats: CleanerStats = new CleanerStats()

    // 退避锁，用于在必要的时候等待一段时间
    private val backOffWaitLatch = new CountDownLatch(1)

    private def checkDone(topicAndPartition: TopicAndPartition) {
      if (!isRunning.get())
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicAndPartition)
    }

    /**
     * The main loop for the cleaner thread
     */
    override def doWork() {
      cleanOrSleep()
    }
    
    
    override def shutdown() = {
    	 initiateShutdown()
    	 backOffWaitLatch.countDown()
    	 awaitShutdown()
     }
     
    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private def cleanOrSleep() {
      // 通过CleanerManager的grabFilthiestLog()方法获取需要进行日志压缩的Log
      cleanerManager.grabFilthiestLog() match {
        case None =>
          // there are no cleanable logs, sleep a while
          // 没有需要压缩的Log，退避一段时间
          backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS)
        case Some(cleanable) =>
          // there's a log, clean it
          // 有需要压缩的Log
          // 获取Log的firstDirtyOffset，即dirty log的起始offset，是clean log和dirty log的分界线
          var endOffset = cleanable.firstDirtyOffset
          try {
            // 调用Cleaner对象进行日志压缩
            endOffset = cleaner.clean(cleanable)
            // 用于记录状态并输出过程日志
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleaner.stats)
          } catch {
            case pe: LogCleaningAbortedException => // task can be aborted, let it go.
          } finally {
            // 完成清理，进行状态转换，更新cleaner-offset-checkpoint文件
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
      }
    }
    
    /**
     * Log out statistics on a single run of the cleaner.
     */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
      this.lastStats = stats
      cleaner.statsUnderlying.swap
      def mb(bytes: Double) = bytes / (1024*1024)
      val message = 
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) + 
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead), 
                                                                                stats.elapsedSecs, 
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) + 
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead), 
                                                                                           stats.elapsedIndexSecs, 
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs, 
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead), 
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs, 
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) + 
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) + 
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead), 
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }
   
  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication SkimpyOffsetMap类型的Map，为dirty部分的消息建立key与last_offset的映射关系
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer. 指定了读写LogSegment的ByteBuffer大小
 * @param maxIoBufferSize The maximum size of a message that can appear in the log 指定的消息的最大长度
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer 指定了SkimpyOffsetMap的最大占用比例
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted. 用来检测Log的压缩状态
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: (TopicAndPartition) => Unit) extends Logging {
  
  override val loggerName = classOf[LogCleaner].getName

  this.logIdent = "Cleaner " + id + ": "
  
  /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
  val statsUnderlying = (new CleanerStats(time), new CleanerStats(time))
  def stats = statsUnderlying._1

  /**
    *  buffer used for read i/o
    *  读缓冲区
    **/
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)

  /**
    *  buffer used for write i/o
    *  写缓冲区
    **/
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned
   */
  private[log] def clean(cleanable: LogToClean): Long = {
    // 重置状态为这一次的压缩做准备
    stats.clear()
    info("Beginning cleaning of log %s.".format(cleanable.log.name))
    // 获取需要压缩的Log对象
    val log = cleanable.log

    // build the offset map
    info("Building offset map for %s...".format(cleanable.log.name))
    // activeSegment不参与压缩，所以activeSegment的baseOffset是可以压缩的最大上限
    val upperBoundOffset = log.activeSegment.baseOffset

    // 第1步：填充OffsetMap，确定日志压缩的真正上限
    val endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1

    // 维护状态
    stats.indexDone()

    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    // 第2步：计算可以安全删除"删除标识"（即value为空的消息）的LogSegment
    val deleteHorizonMs =
      log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - log.config.deleteRetentionMs
    }

    // group the segments and clean the groups
    info("Cleaning log %s (discarding tombstones prior to %s)...".format(log.name, new Date(deleteHorizonMs)))
    // 第3步：对要压缩的LogSegment进行分组，按照分组进行压缩
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
      // 第4步：开始进行压缩
      cleanSegments(log, group, offsetMap, deleteHorizonMs)

    // record buffer utilization
    // 记录buffer使用率
    stats.bufferUtilization = offsetMap.utilization

    // 维护状态
    stats.allDone()

    // 返回压缩的真正上限
    endOffset
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment], 
                                 map: OffsetMap, 
                                 deleteHorizonMs: Long) {
    // create a new segment with the suffix .cleaned appended to both the log and index name
    // 创建新的日志文件和索引，文件名是分组中第一个LogSegment所在的文件的名称后面加上.cleaned后缀为新文件名
    val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix)
    logFile.delete()
    val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix)
    indexFile.delete()

    // 根据logFile创建FileMessageSet对象
    val messages = new FileMessageSet(logFile, fileAlreadyExists = false, initFileSize = log.initFileSize(), preallocate = log.config.preallocate)
    // 根据indexFile创建OffsetIndex对象
    val index = new OffsetIndex(indexFile, segments.head.baseOffset, segments.head.index.maxIndexSize)
    // 根据FileMessageSet和OffsetIndex创建LogSegment对象
    val cleaned = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, log.config.randomSegmentJitter, time)

    try {
      // clean segments into the new destination segment
      // 遍历一组内的LogSegment
      for (old <- segments) {
        // 判定此LogSegment中"删除标记"是否可以安全删除
        val retainDeletes = old.lastModified > deleteHorizonMs
        info("Cleaning segment %s in log %s (last modified %s) into %s, %s deletes."
            .format(old.baseOffset, log.name, new Date(old.lastModified), cleaned.baseOffset, if(retainDeletes) "retaining" else "discarding"))
        // 进行日志压缩
        cleanInto(log.topicAndPartition, old, cleaned, map, retainDeletes, log.config.messageFormatVersion.messageFormatVersion)
      }

      // trim excess index
      // 截除多余的索引项
      index.trimToValidSize()

      // flush new segment to disk before swap
      // 将压缩得到的LogSegment进行刷盘
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      // 修改LogSegment的最后修改时间
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info("Swapping in cleaned segment %d for segment(s) %s in log %s.".format(cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))

      /**
        * 替换原来的LogSegment
        * 首先将.cleaned文件改为.swap后缀，将LogSegment加入到Log.segments跳表
        * 从Log.segments跳表中删除旧的LogSegment（对应的文件也会被删除）
        * 将.swap文件的.swap后缀删除
        */
      log.replaceSegments(cleaned, segments)
    } catch {
      case e: LogCleaningAbortedException =>
        // 如果出错，删除cleaned文件
        cleaned.delete()
        throw e
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param source The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param messageFormatVersion the message format version to use after compaction
   */
  private[log] def cleanInto(topicAndPartition: TopicAndPartition,
                             source: LogSegment,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainDeletes: Boolean,
                             messageFormatVersion: Byte) {
    var position = 0
    // 遍历LogSegment中日志数据
    while (position < source.log.sizeInBytes) {
      // 检查状态
      checkDone(topicAndPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      // 清除readBuffer和writeBuffer
      readBuffer.clear()
      writeBuffer.clear()
      // 将LogSegment的日志数据读到readBuffer中，然后根据该装有数据的buffer创建ByteBufferMessageSet对象
      val messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position))
      // 限速
      throttler.maybeThrottle(messages.sizeInBytes)
      // check each message to see if it is to be retained
      var messagesRead = 0
      // 对读取得到的ByteBufferMessageSet进行遍历，浅层迭代
      for (entry <- messages.shallowIterator) {
        // 得到消息大小
        val size = MessageSet.entrySize(entry.message)
        stats.readMessage(size)
        // 判断消息是否使用了压缩器，如果使用了压缩器，需要深层迭代
        if (entry.message.compressionCodec == NoCompressionCodec) {
          // 未使用压缩
          /**
            * shouldRetainMessage方法会根据OffsetMap、retainDeletes在原LogSegment中判断entry是否需要保留
            * 条件如下：
            * 1. entry消息是否有键，无键表示是无效消息，可以删除；
            * 2. OffsetMap中是否有与entry的键相同的且offset更大的消息，如果是表示entry是过期消息
            * 3. entry消息被"删除标记"且LogSegment配置为"删除标记"可以安全删除，可以删除的前提条件
            */
          if (shouldRetainMessage(source, map, retainDeletes, entry)) {
            // 需要保留消息
            ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset)
            stats.recopyMessage(size)
          }
          messagesRead += 1
        } else {
          // 使用了压缩器，需要深层迭代
          // We use the absolute offset to decide whether to retain the message or not. This is handled by the
          // deep iterator.
          val messages = ByteBufferMessageSet.deepIterator(entry)
          var writeOriginalMessageSet = true
          val retainedMessages = new mutable.ArrayBuffer[MessageAndOffset]
          // 遍历深层消息
          messages.foreach { messageAndOffset =>
            messagesRead += 1
            // 判断消息是否保留
            if (shouldRetainMessage(source, map, retainDeletes, messageAndOffset))
              retainedMessages += messageAndOffset
            else writeOriginalMessageSet = false // 一旦有消息不保留，则置writeOriginalMessageSet为false
          }

          // There are no messages compacted out, write the original message set back
          if (writeOriginalMessageSet)
          // 如果writeOriginalMessageSet为true，表示内部压缩消息没有需要清理的，直接将Message写出即可
            ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset)
          else
            // 否则调用compressMessages()重新压缩retainedMessages集合，同时写入到writeBuffer
            compressMessages(writeBuffer, entry.message.compressionCodec, messageFormatVersion, retainedMessages)
        }
      }
      // 维护position位置
      position += messages.validBytes
      // if any messages are to be retained, write them out
      // 如果有需要保留的消息，将其追加到压缩后的LogSegment中
      if (writeBuffer.position > 0) {
        writeBuffer.flip()
        val retained = new ByteBufferMessageSet(writeBuffer)
        dest.append(retained.head.offset, retained)
        throttler.maybeThrottle(writeBuffer.limit)
      }
      
      // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
      // 未读取到完整的消息，readBuffer可能过小，进行扩容
      if (readBuffer.limit > 0 && messagesRead == 0)
        growBuffers()
    }
    // 重置readBuffer和writeBuffer
    restoreBuffers()
  }

  private def compressMessages(buffer: ByteBuffer,
                               compressionCodec: CompressionCodec,
                               messageFormatVersion: Byte,
                               messageAndOffsets: Seq[MessageAndOffset]) {
    require(compressionCodec != NoCompressionCodec, s"compressionCodec must not be $NoCompressionCodec")
    if (messageAndOffsets.nonEmpty) {
      val messages = messageAndOffsets.map(_.message)
      val magicAndTimestamp = MessageSet.magicAndLargestTimestamp(messages)
      val firstMessageOffset = messageAndOffsets.head
      val firstAbsoluteOffset = firstMessageOffset.offset
      var offset = -1L
      val timestampType = firstMessageOffset.message.timestampType
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = messageFormatVersion) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, messageFormatVersion, outputStream))
        try {
          for (messageOffset <- messageAndOffsets) {
            val message = messageOffset.message
            offset = messageOffset.offset
            if (messageFormatVersion > Message.MagicValue_V0) {
              // The offset of the messages are absolute offset, compute the inner offset.
              val innerOffset = messageOffset.offset - firstAbsoluteOffset
              output.writeLong(innerOffset)
            } else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      ByteBufferMessageSet.writeMessage(buffer, messageWriter, offset)
      stats.recopyMessage(messageWriter.size + MessageSet.LogOverhead)
    }
  }

  // 返回false表示可以删除消息，true表示保留消息
  private def shouldRetainMessage(source: kafka.log.LogSegment,
                                  map: kafka.log.OffsetMap,
                                  retainDeletes: Boolean,
                                  entry: kafka.message.MessageAndOffset): Boolean = {
    // 获取entry的键
    val key = entry.message.key
    if (key != null) {
      // 键不为空，从OffsetMap中查找该键记录的消息的offset
      val foundOffset = map.get(key)
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       *
       */
      // 如果OffsetMap中获取的offset比entry的offset还要大，说明entry消息是旧消息，可以被删除
      val redundant = foundOffset >= 0 && entry.offset < foundOffset
      // 根据LogSegment配置的"删除标记"策略以及消息数据是否为空来决定是否删除
      val obsoleteDelete = !retainDeletes && entry.message.isNull
      !redundant && !obsoleteDelete
    } else {
      // 键为空，为无效消息，返回false
      stats.invalidMessage()
      false
    }
  }

  /**
   * Double the I/O buffer capacity
    * 对readBuffer和writeBuffer进行扩容，新容量为旧容量的2倍
   */
  def growBuffers() {
    // 检查是否过度扩容
    if(readBuffer.capacity >= maxIoBufferSize || writeBuffer.capacity >= maxIoBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxIoBufferSize))
    // 新容量
    val newSize = math.min(this.readBuffer.capacity * 2, maxIoBufferSize)
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
    // 扩容
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }
  
  /**
   * Restore the I/O buffer capacity to its original size
   */
  def restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
    *
    * 对LogSegment进行分组，将相同主题分区的LogSegment分到一组
   *
   * @param segments The log segments to group 需要分组的LogSegment集合
   * @param maxSize the maximum size in bytes for the total of all log data in a group 一个分组最大的字节数
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group 一个分组最大的索引字节数
   *
   * @return A list of grouped segments
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int): List[Seq[LogSegment]] = {
    // 创建容器
    var grouped = List[List[LogSegment]]()
    var segs = segments.toList
    // 遍历LogSegment数组
    while(!segs.isEmpty) {
      // 头LogSegment
      var group = List(segs.head)
      // 头LogSegment的大小
      var logSize = segs.head.size
      // 头LogSegment的索引大小
      var indexSize = segs.head.index.sizeInBytes
      // 截除头LogSegment
      segs = segs.tail
      while(!segs.isEmpty && // 还剩余有LogSegment
            logSize + segs.head.size <= maxSize && // 检查logSize是否过大
            indexSize + segs.head.index.sizeInBytes <= maxIndexSize && // 检查indexSize是否过大
        /**
          * 剩余LogSegment的头LogSegment的最后一个offset索引
          * 与刚刚截除的头LogSegment的baseOffset索引的差值
          * 小于等于Int.MaxValue时，表示是同一个LogSegment
          */
        segs.head.index.lastOffset - group.last.index.baseOffset <= Int.MaxValue) {
        // 将头LogSegment添加到group头部
        group = segs.head :: group
        // 维护logSize和indexSize
        logSize += segs.head.size
        indexSize += segs.head.index.sizeInBytes
        // 截除头LogSegment
        segs = segs.tail
      }
      // 此时group已经是分好的一组了，将group反转后添加到grouped中
      grouped ::= group.reverse
    }
    // 将grouped反转后返回
    grouped.reverse
  }

  /**
   * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   *
   * @return The final offset the map covers
   */
  private[log] def buildOffsetMap(log: Log, start: Long, end: Long, map: OffsetMap): Long = {
    // 清理传入的map
    map.clear()
    // 查找从[start, end)之间所有的LogSegment
    val dirty = log.logSegments(start, end).toBuffer
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

    // Add all the dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    // 起始LogSegment的baseOffset
    var offset = dirty.head.baseOffset
    require(offset == start, "Last clean offset is %d but segment base offset is %d for log %s.".format(start, offset, log.name))
    // 标识OffsetMap是否被填满
    var full = false
    // 在OffsetMap未满时，遍历所有的dirty LogSegment
    for (segment <- dirty if !full) {
      /**
        * 检查LogCleanerManager记录的该分区的压缩状态，在该方法中
        * 如果清理线程CleanerThread停止了，会抛出ThreadShutdownException异常；
        * 如果LogSegment的压缩状态为LogCleaningAborted，会抛出LogCleaningAbortedException
        */
      checkDone(log.topicAndPartition)
      // 处理单个LogSegment，将消息的key和offset添加到OffsetMap中
      val newOffset = buildOffsetMapForSegment(log.topicAndPartition, segment, map)
      if (newOffset > -1L) // OffsetMap未满，更新offset为newOffset
        offset = newOffset
      else {
        // OffsetMap已满，此时如果offset大于start，表示OffsetMap中记录了最后一个LogSegment的一部分消息
        // If not even one segment can fit in the map, compaction cannot happen
        require(offset > start, "Unable to build the offset map for segment %s/%s. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads".format(log.name, segment.log.file.getName))
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
        // 标记OffsetMap已填满
        full = true
      }
    }
    info("Offset map for log %s complete.".format(log.name))

    /**
      * 返回offset，即本次日志压缩的结尾，
      * 由于buildOffsetMapForSegment()方法的控制，不会出现半个LogSegment的情况
      */
    offset
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   *
   * @return The final offset covered by the map or -1 if the map is full
   */
  private def buildOffsetMapForSegment(topicAndPartition: TopicAndPartition, segment: LogSegment, map: OffsetMap): Long = {
    var position = 0
    var offset = segment.baseOffset
    // 计算OffsetMap最大可装载的key和offset对的数量
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    // 遍历LogSegment
    while (position < segment.log.sizeInBytes) {
      // 检查压缩状态
      checkDone(topicAndPartition)
      readBuffer.clear()

      // 从LogSegment中读取消息数据，并将其构造为ByteBufferMessageSet对象
      val messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position))
      // 限速设置
      throttler.maybeThrottle(messages.sizeInBytes)
      val startPosition = position

      // 遍历messages中的消息，可能会进行深层迭代
      for (entry <- messages) {
        val message = entry.message
        if (message.hasKey) {
          // 只会处理有键的消息
          if (map.size < maxDesiredMapSize)
            // 将key和offset放入OffsetMap
            map.put(message.key, entry.offset)
          else {
            // OffsetMap填满了
            // The map is full, stop looping and return
            return -1L
          }
        }
        // 更新offset
        offset = entry.offset
        // 维护记录处理的消息数量
        stats.indexMessagesRead(1)
      }

      // 移动position，准备下一次读取
      position += messages.validBytes
      // 维护记录处理的消息字节数
      stats.indexBytesRead(messages.validBytes)

      // if we didn't read even one complete message, our read buffer may be too small
      /**
        * 如果position没有移动，还是与startPosition一致，说明没有读取到一个完整的Message，
        * 可能是由于readBuffer和writeBuffer容量不够用，因此对readBuffer和writeBuffer进行扩容后重新读取
        * 扩容后的新容量是旧容量的2倍，但不超过maxIoBufferSize
        */
      if(position == startPosition)
        growBuffers()
    }
    // 重置readBuffer和writeBuffer的大小
    restoreBuffers()
    // 返回LogSegment的最后一个消息的offset
    offset
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private case class CleanerStats(time: Time = SystemTime) {
  var startTime, mapCompleteTime, endTime, bytesRead, bytesWritten, mapBytesRead, mapMessagesRead, messagesRead,
      messagesWritten, invalidMessagesRead = 0L
  var bufferUtilization = 0.0d
  clear()
  
  def readMessage(size: Int) {
    messagesRead += 1
    bytesRead += size
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }
  
  def recopyMessage(size: Int) {
    messagesWritten += 1
    bytesWritten += size
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }
  
  def elapsedSecs = (endTime - startTime)/1000.0
  
  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0
  
  def clear() {
    startTime = time.milliseconds
    mapCompleteTime = -1L
    endTime = -1L
    bytesRead = 0L
    bytesWritten = 0L
    mapBytesRead = 0L
    mapMessagesRead = 0L
    messagesRead = 0L
    invalidMessagesRead = 0L
    messagesWritten = 0L
    bufferUtilization = 0.0d
  }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
private case class LogToClean(topicPartition: TopicAndPartition, log: Log, firstDirtyOffset: Long) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size).sum
  val dirtyBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, log.activeSegment.baseOffset)).map(_.size).sum
  val cleanableRatio = dirtyBytes / totalBytes.toDouble
  def totalBytes = cleanBytes + dirtyBytes
  // 比较方法，通过cleanableRatio值来比较
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
