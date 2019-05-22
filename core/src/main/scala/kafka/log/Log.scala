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

import kafka.utils._
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import java.io.{File, IOException}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import java.text.NumberFormat

import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException}
import org.apache.kafka.common.record.TimestampType

import scala.collection.JavaConversions
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Utils

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, Message.NoTimestamp, NoCompressionCodec, NoCompressionCodec, -1, -1, false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 * @param firstOffset The first offset in the message set 第一条消息的offset
 * @param lastOffset The last offset in the message set 最后一条消息的offset
 * @param timestamp The log append time (if used) of the message set, otherwise Message.NoTimestamp 时间戳
 * @param sourceCodec The source codec used in the message set (send by the producer) 生产者采用的压缩方式
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any) 服务端采用的压缩方式
 * @param shallowCount The number of shallow messages 外层消息个数
 * @param validBytes The number of valid bytes 通过验证的总字节数
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing 内部消息的offset是否是单调递增的
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var timestamp: Long,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean)


/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created. Log对应的磁盘目录，此目录下存放了每个LogSegment对应的日志文件和索引文件
 * @param config The log configuration settings Log相关的配置信息
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk 指定恢复操作的起始offset，recoveryPoint之前的Message已经刷新到磁盘上持久存储，而其后的消息则不一定，出现宕机时可能会丢失。所以只需要恢复recoveryPoint之后的消息即可。
 * @param scheduler The thread pool scheduler used for background actions 线程池调度器，用于后台操作
 * @param time The time instance used for checking the clock
 *
 */
@threadsafe
class Log(val dir: File,
          @volatile var config: LogConfig,
          @volatile var recoveryPoint: Long = 0L,
          scheduler: Scheduler,
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /**
    * A lock that guards all modifications to the log
    * 可能存在多个Handler线程并发向同一个Log追加消息，所以对Log的修改操作需要进行同步
    * */
  private val lock = new Object

  /**
    * last time it was flushed
    * 最后一次刷盘时间
    * */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  /**
    * the actual segments of the log
    * 用于对LogSegment进行管理
    * 键是LogSegment的baseOffset，值是LogSegment对象
    * */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  // 加载LogSegment
  loadSegments()

  /**
    * Calculate the offset of the next message
    * LogOffsetMetadata对象。主要用于产生分配给消息的offset，同时也是当前副本的LEO（LogEndOffset）。
    *   - messageOffset字段记录了Log中最后一个offset值
    *   - segmentBaseOffset字段记录了activeSegment的baseOffset
    *   - relativePositionInSegment字段记录了activeSegment的大小
    * */
  @volatile var nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset(), activeSegment.baseOffset, activeSegment.size.toInt)

  val topicAndPartition: TopicAndPartition = Log.parseTopicPartitionName(dir)

  info("Completed load of log %s with log end offset %d".format(name, logEndOffset))

  val tags = Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString)

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  /** The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments() {
    // create the log directory if it doesn't exist
    dir.mkdirs()
    var swapFiles = Set[File]()

    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // 遍历dir目录下的文件，处理.cleaned、.deleted、.swap文件
    for(file <- dir.listFiles if file.isFile) {
      // 文件不可读，抛异常
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // if the file ends in .deleted or .cleaned, delete it
        // 文件是以.deleted或.cleaned后缀结尾，则直接删除；
        // .deleted表示本就标记为要删除的文件；.cleaned文件表示压缩过程出现宕机
        file.delete()
      } else if(filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        // 文件是以.swap后缀结尾；.swap文件表示压缩完的完整消息，可以使用，将其后缀去掉
        val baseName = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        if(baseName.getPath.endsWith(IndexFileSuffix)) {
          // 如果该.swap文件是索引文件，直接删除
          file.delete()
        } else if(baseName.getPath.endsWith(LogFileSuffix)){
          // delete the index
          // 如果该.swap文件是日志文件，则需要进行恢复；获取该日志文件对应的索引文件，将其删除
          val index = new File(CoreUtils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix))
          index.delete()
          // 将该文件添加到swapFiles集合
          swapFiles += file
        }
      }
    }

    // now do a second pass and load all the .log and .index files
    /**
      * 加载全部的日志文件及索引文件，
      *   - 如果存在没有对应的日志文件的索引文件，就将该索引文件也删除
      *   - 如果存在没有对应的索引文件的日志文件，则为其重建索引文件
      */
    for(file <- dir.listFiles if file.isFile) {
      val filename = file.getName
      if(filename.endsWith(IndexFileSuffix)) {
        // if it is an index file, make sure it has a corresponding .log file
        // 是.index文件，判断对应的.log文件是否存在，如果不存在就将.index文件也删除
        val logFile = new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))
        if(!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          file.delete()
        }
      } else if(filename.endsWith(LogFileSuffix)) {
        // if its a log file, load the corresponding log segment
        // 是.loh文件，先创建并加载LogSegment
        val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
        val indexFile = Log.indexFilename(dir, start)
        val segment = new LogSegment(dir = dir,
                                     startOffset = start,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = true)
        // 判断对应的.index文件是否存在，如果不存在就根据.log文件重建.index文件
        if(indexFile.exists()) {
          // .index文件存在
          try {
              // 检查索引文件的完整性
              segment.index.sanityCheck()
          } catch {
            case e: java.lang.IllegalArgumentException =>
              warn("Found a corrupted index file, %s, deleting and rebuilding index...".format(indexFile.getAbsolutePath))
              indexFile.delete()
              segment.recover(config.maxMessageSize)
          }
        }
        else {
          // .index文件不存在
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          // 根据.log文件重建.index文件
          segment.recover(config.maxMessageSize)
        }
        // 将LogSegment添加到segments跳表中
        segments.put(start, segment)
      }
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    // 处理前面得到的.swap文件集合
    for (swapFile <- swapFiles) {
      // 去掉.swap后缀的文件
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val fileName = logFile.getName
      // 根据文件名得到baseOffset
      val startOffset = fileName.substring(0, fileName.length - LogFileSuffix.length).toLong
      // 获取对应的.index.swap文件
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      // 根据上面得到的信息创建OffsetIndex
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      // 创建LogSegment
      val swapSegment = new LogSegment(new FileMessageSet(file = swapFile),
                                       index = index,
                                       baseOffset = startOffset,
                                       indexIntervalBytes = config.indexInterval,
                                       rollJitterMs = config.randomSegmentJitter,
                                       time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      // 重建索引文件并验证日志文件
      swapSegment.recover(config.maxMessageSize)
      // 查找swapSegment对应的日志压缩前的LogSegment集合
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset)
      /**
        * 将swapSegment放入segments跳表，
        * 将oldSegments的LogSegment从segments跳表中删除，同时删除对应的日志文件和索引文件
        * 将swapSegment对应日志文件的.swap后缀去掉
        */
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }

    // 如果segments为空，需要添加一个LogSegment作为activeSegment
    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
    } else {
      // 进行Log的恢复工作
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.index.resize(config.maxIndexSize)
    }

  }

  private def updateLogEndOffset(messageOffset: Long) {
    // 每次更新都会创建新的LogOffsetMetadata对象，LogOffsetMetadata是个不可变对象
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }

  /**
    * 进行Log的恢复工作，主要负责处理Broker非正常关闭时导致的消息异常，
    * 需要将recoveryPoint ~ activeSegment中的所有消息进行验证，将验证失败的消息截断
    */
  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    // 如果broker上次是正常关闭的，则不需要恢复
    if(hasCleanShutdownFile) {
      // 更新recoveryPoint为activeSegment的nextOffset
      this.recoveryPoint = activeSegment.nextOffset
      return
    }

    // okay we need to actually recovery this log
    // 非正常关闭，需要进行恢复操作，得到recoveryPoint点开始的LogSegment集合
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    // 遍历LogSegment集合
    while(unflushed.hasNext) {
      val curr = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
      val truncatedBytes =
        try {
          // 使用LogSegment的recover()方法重建索引文件并验证日志文件，验证失败的部分会被截除
          curr.recover(config.maxMessageSize)
        } catch {
          case e: InvalidOffsetException =>
            val startOffset = curr.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            curr.truncateTo(startOffset)
        }
      // LogSegment中是否有验证失败的消息
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
        // 将剩余的LogSegment全部删除
        unflushed.foreach(deleteSegment)
      }
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile() = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      for(seg <- logSegments)
        seg.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
    *
    * 向Log追加消息；
    * Kafka服务端在处理生产者发来的ProducerRequest时，会将请求解析成ByteBufferMessageSet，并最终调用append()方法完成追加消息
   *
   * @param messages The message set to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   *
   * @throws KafkaStorageException If the append fails due to an I/O error.
   *
   * @return Information about the appended messages including the first and last offset.
   */
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): LogAppendInfo = {
    // 对ByteBufferMessageSet类型的messages中的Message数据进行验证
    val appendInfo = analyzeAndValidateMessageSet(messages) // 返回LogAppendInfo类型的对象

    // if we have any valid messages, append them to the log
    // shallowCount为0表示messages中没有消息
    if (appendInfo.shallowCount == 0)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    // 在添加到磁盘日志文件中之前删除无用的字节或部分消息
    var validMessages = trimInvalidBytes(messages, appendInfo)

    try {
      // they are valid, insert them in the log
      // synchronized加锁
      lock synchronized {

        if (assignOffsets) { // 是否需要分配offset，默认是需要的
          // assign offsets to the message set
          /**
            * 获取Log中最后一个offset，从该offset开始向后分配
            * LongRef内部封装了一些便捷的方法
            */
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          // 使用appendInfo的firstOffset记录第一条消息的offset
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          val (validatedMessages, messageSizesMaybeChanged) = try {
            /**
              * 验证并分配offset，这个是ByteBufferMessageSet的方法，返回值为(ByteBufferMessageSet, Boolean)
              * 注意，在这个过程中，offset（LongRef类型）的value会根据消息的分配而改变，最终变为最后一条消息的offset + 1
              * 具体操作可以看ByteBufferMessageSet的validateMessagesAndAssignOffsets()方法
              */
            validMessages.validateMessagesAndAssignOffsets(offset,
                                                           now,
                                                           appendInfo.sourceCodec,
                                                           appendInfo.targetCodec,
                                                           config.compact,
                                                           config.messageFormatVersion.messageFormatVersion,
                                                           config.messageTimestampType,
                                                           config.messageTimestampDifferenceMaxMs)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          // 记录已经通过验证并分配了offset的ByteBufferMessageSet
          validMessages = validatedMessages
          // 使用appendInfo的lastOffset记录最后一条消息的offset
          appendInfo.lastOffset = offset.value - 1
          // 如果需要配置了需要添加日志添加时间，就在appendInfo中添加时间戳
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.timestamp = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          // 如果在ByteBufferMessageSet的validateMessagesAndAssignOffsets()方法中修改了ByteBufferMessageSet的长度，则需要重新检测消息长度
          if (messageSizesMaybeChanged) {
            // 遍历消息
            for (messageAndOffset <- validMessages.shallowIterator) {
              // 检查每个消息的大小是否大于配置的最大消息大小阈值
              if (MessageSet.entrySize(messageAndOffset.message) > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
                throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
                  .format(MessageSet.entrySize(messageAndOffset.message), config.maxMessageSize))
              }
            }
          }

        } else {
          // we are taking the offsets we are given
          /**
            * 如果不需要分配offset，检查边界
            * - 消息offset不是单调递增的
            * - 第一条消息的offset小于Log中最后一个offset值
            */
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + messages)
        }

        // check messages set size may be exceed config.segmentSize
        // 检查所有消息的总大小是否大于单个文件的大小阈值
        if (validMessages.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message set size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validMessages.sizeInBytes, config.segmentSize))
        }

        // maybe roll the log if this segment is full
        // 可能需要进行滚动操作，即当当前文件剩余大小无法满足本次消息写入时，需要新创建一个activeSegment文件
        val segment = maybeRoll(validMessages.sizeInBytes)

        // now append to the log
        // 追加消息
        segment.append(appendInfo.firstOffset, validMessages)

        // increment the log end offset
        // 更新LEO
        updateLogEndOffset(appendInfo.lastOffset + 1)

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validMessages))

        // 检查未刷新到磁盘的数据是否达到一定阈值，如果是则调用flush()刷新
        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset, lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    // 遍历messages进行验证
    for(messageAndOffset <- messages.shallowIterator) { // 每个都是MessageAndOffset对象
      // update the first offset if on the first message
      if(firstOffset < 0)
        // 记录第一条消息额offset
        firstOffset = messageAndOffset.offset
      // check that offsets are monotonically increasing
      // 检查offset是否是单调自增的
      if(lastOffset >= messageAndOffset.offset)
        monotonic = false
      // update the last offset seen
      // 更新lastOffset为当前消息的offset
      lastOffset = messageAndOffset.offset

      // 获取消息
      val m = messageAndOffset.message

      // Check if the message sizes are valid.
      // 检查消息大小是否合法
      val messageSize = MessageSet.entrySize(m)
      if(messageSize > config.maxMessageSize) {
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
        // 消息过大，抛出RecordTooLargeException异常
        throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
          .format(messageSize, config.maxMessageSize))
      }

      // check the validity of the message by checking CRC
      // 检查CRC
      m.ensureValid()

      // 更新浅层迭代数量+1
      shallowMessageCount += 1
      // 更新有效字节数
      validBytesCount += messageSize

      // 记录压缩器
      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    // 获取压缩器
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)

    // 根据上面的信息构建LogAppendInfo对象并返回
    LogAppendInfo(firstOffset, lastOffset, Message.NoTimestamp, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   * @param messages The message set to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(messages: ByteBufferMessageSet, info: LogAppendInfo): ByteBufferMessageSet = {
    val messageSetValidBytes = info.validBytes
    // 如果有效字节数为0，抛出CorruptRecordException异常
    if(messageSetValidBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if(messageSetValidBytes == messages.sizeInBytes) {
      // 有效字节数与消息大小相同，说明没有无效字节，直接返回即可
      messages
    } else {
      // trim invalid bytes
      // 创建一个副本
      val validByteBuffer = messages.buffer.duplicate()
      // 只取有效字节，将之后的字节全部丢弃，滤得有效字节
      validByteBuffer.limit(messageSetValidBytes)
      // 根据滤得的有效字节创建ByteBufferMessageSet对象并返回
      new ByteBufferMessageSet(validByteBuffer)
    }
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at 开始读取的startOffset
   * @param maxLength The maximum number of bytes to read 最大读取的数据字节数
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set) 最大读取到的offset（不包含）
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // Because we don't use lock for reading, the synchronization is a little bit tricky.
    // We create the local variables to avoid race conditions with updates to the log.
    /**
      * read操作不会加锁，因此将nextOffsetMetadata拷贝一份为局部变量避免线程竞争更新了nextOffsetMetadata
      * nextOffsetMetadata每次在updateLogEndOffset()方法的代码中更新updateLogEndOffset的时候，都是创建新的LogOffsetMetadata对象，
      * 而且LogOffsetMetadata中也没有提供任何修改属性的方法，可见LogOffsetMetadata对象是个不可变对象
      */
    val currentNextOffsetMetadata = nextOffsetMetadata
    // 下一条记录的offset
    val next = currentNextOffsetMetadata.messageOffset
    // 如果起始offset等于下一条记录的offset，说明读取的数据其实是空的，返回一个MessageSet为空的FetchDataInfo对象
    if(startOffset == next)
      return FetchDataInfo(currentNextOffsetMetadata, MessageSet.Empty)

    // 查找offset仅小于startOffset的LogSegment对象
    var entry = segments.floorEntry(startOffset)

    // attempt to read beyond the log end offset is an error
    // 检查边界
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      // 当查找到的entry不为null
      // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
      // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
      // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
      // end of the active segment.
      /**
        * 如果读取的entry是activeSegment，可能会有并发线程在读取期间添加了消息数据导致activeSegment发生改变
        */
      val maxPosition = {
        if (entry == segments.lastEntry) { // 是否是segments集合中最后一个元素，即读取的是activeSegment
          // 获取activeSegment表示的物理日志存储的偏移量
          val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
          // Check the segment again in case a new segment has just rolled out.
          // 再次检查当前的activeSegment是否被改变
          if (entry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
            // 写线程并发进行了roll()操作，此时activeSegment改变了，变成了一个新的LogSegment
            entry.getValue.size
          else
            // activeSegment没有改变，直接返回exposedPos
            exposedPos
        } else {
          // 读取的是非activeSegment的情况，可以直接读取到LogSegment的结尾
          entry.getValue.size
        }
      }
      // 使用LogSegment的read()方法读取消息数据
      val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition)
      if(fetchInfo == null) {
        // 如果在这个LogSegment中没有读取到数据，就继续读取下一个LogSegment
        entry = segments.higherEntry(entry.getKey)
      } else {
        // 否则直接返回
        return fetchInfo
      }
    }

    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // In this case, we will return the empty set with log end offset metadata
    // 找不到startOffset之后的消息，返回一个MessageSet为空的FetchDataInfo对象
    FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case e: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized { // 加锁
      //find any segments that match the user-supplied predicate UNLESS it is the final segment
      //and it is empty (since we would just end up re-creating it)
      // 获取activeSegment
      val lastEntry = segments.lastEntry
      val deletable =
        if (lastEntry == null) Seq.empty // 没有activeSegment
        /**
          * 得到segments跳表中value集合的迭代器
          * 循环检测LogSegment是否符合删除条件，并将符合条件的LogSegment形成集合返回
          * takeWhile会根据传入的条件（A => Boolean）筛选元素，条件如下：
          *   - s => predicate(s)：使用predicate判断s是否符合条件
          *   - (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0)
          *     - s.baseOffset != lastEntry.getValue.baseOffset：s是否不是activeSegment
          *     - s.size > 0：s的大小大于0
          * 即在判断时间是否超时的情况下：
          * 1. LogSegment的最近修改日期在retention.ms之前；
          * 2. LogSegment不是activeSegment
          * 3. LogSegment的大小大于0
          * 即在判断大小是否超额的情况下：
          * 1. Log的大小已经大于retention.bytes，且LogSegment的大小小于前面二者的差额；
          * 2. LogSegment不是activeSegment
          * 3. LogSegment的大小大于0
          */
        else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
      // 需要删除的LogSegment数量
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        // 全部的LogSegment都需要删除
        if (segments.size == numToDelete)
          // 删除前先创建一个新的activeSegment，保证保留一个LogSegment
          roll()
        // remove the segments for lookups
        // 遍历删除LogSegment
        deletable.foreach(deleteSegment(_))
      }
      // 返回删除的数量
      numToDelete
    }
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

   /**
   * The earliest message offset in the log
   */
  def logStartOffset: Long = logSegments.head.baseOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   *  The offset of the next message that will be appended to the log
    *  LEO
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int): LogSegment = {
    val segment = activeSegment
    /**
      * 需要触发roll操作的条件有三个，满足一个即需要roll：
      * - 当前activeSegment的日志大小加上本次待追加的消息集合大小，超过配置的LogSegment的最大长度。
      * - 当前activeSegment的寿命超过了配置的LogSegment最长存活时间。
      * - 索引文件满了。
      */
    if (segment.size > config.segmentSize - messagesSize || // 当前activeSegment的日志大小加上本次待追加的消息集合大小，超过配置的LogSegment的最大长度。
        segment.size > 0 && time.milliseconds - segment.created > config.segmentMs - segment.rollJitterMs || // 当前activeSegment的寿命超过了配置的LogSegment最长存活时间。
        segment.index.isFull) { // 索引文件满了。
      debug("Rolling new log segment in %s (log_size = %d/%d, index_size = %d/%d, age_ms = %d/%d)."
            .format(name,
                    segment.size,
                    config.segmentSize,
                    segment.index.entries,
                    segment.index.maxEntries,
                    time.milliseconds - segment.created,
                    config.segmentMs - segment.rollJitterMs))
      // 调用roll()方法滚动，得到新的activeSegment并返回
      roll()
    } else {
      // 不需要roll，直接返回当前activeSegment
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
    * 进行滚动操作
   * @return The newly rolled segment
   */
  def roll(): LogSegment = {
    // 记录开始时间
    val start = time.nanoseconds
    // 加锁
    lock synchronized {
      // 获取LEO
      val newOffset = logEndOffset
      // 根据LEO创建新的日志文件，名称为LEO值.log
      val logFile = logFilename(dir, newOffset)
      // 根据LEO创建新的索引文件，名称为LEO值.index
      val indexFile = indexFilename(dir, newOffset)
      // 判断需要创建的文件是否已存在，如果已存在就删除
      for(file <- List(logFile, indexFile); if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }

      // 当前最后一个LogSegment，也即是旧的activeSegment
      segments.lastEntry() match {
        case null =>
        case entry => {
          // 对索引文件和日志文件都进行截断，保证文件中只保存了有效字节，这对预分配的文件尤其重要
          entry.getValue.index.trimToValidSize()
          entry.getValue.log.trim()
        }
      }

      // 根据创建的文件创建新的LogSegment对象
      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   indexIntervalBytes = config.indexInterval,
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      // 将新创建的LogSegment对象添加到segments集合中
      val prev = addSegment(segment)
      if(prev != null) // ConcurrentSkipListMap的put方法在添加时，如果键已经存在，会把旧值返回，并放弃插入；否则成功插入后返回null
        // 之前就存在对应的baseOffset的LogSegment，抛出异常
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      // 更新nextOffsetMetadata，这次更新的目的是为了更新其中记录的activeSegment.baseOffset和activeSegment.size，而LEO并不会改变
      updateLogEndOffset(nextOffsetMetadata.messageOffset)
      // schedule an asynchronous flush of the old segment
      // 提交执行flush()操作的定时任务
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      // 返回新建的activeSegment
      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    // 将所有LogSegment的 revocerPoint ~ LEO 之间的消息刷新到磁盘上
    for(segment <- logSegments(this.recoveryPoint, offset))
      segment.flush()
    lock synchronized {
      if(offset > this.recoveryPoint) {
        // 更新recoverPoint的值
        this.recoveryPoint = offset
        // 更新最后一次刷盘时间记录
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      removeLogMetrics()
      logSegments.foreach(_.delete())
      segments.clear()
      Utils.delete(dir)
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  private[log] def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment(_))
        activeSegment.truncateTo(targetOffset)
        updateLogEndOffset(targetOffset)
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment(_))
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get

  /**
   * The active segment that is currently taking appends
    * 由于Kafka消息是顺序写的，因此只有最后一个日志文件是处于可写状态的
    * activeSegment用于获取最后一个LogSegment
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = {
    import JavaConversions._
    segments.values
  }

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    import JavaConversions._
    lock synchronized {
      // 找出key仅大于from的最近的元素的键，也即是baseOffset仅大于from的最近的LogSegment对象的baseOffset
      val floor = segments.floorKey(from)
      if(floor eq null)
        // 如果floor为null，则取baseOffset小于to的LogSegment对象
        segments.headMap(to).values
      else
        // 否则取floor到to之间的LogSegment对象
        segments.subMap(floor, true, to, false).values
    }
  }

  override def toString() = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
    *
    * 删除指定的LogSegment
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    // 加锁
    lock synchronized {
      // 从segments集合中移除
      segments.remove(segment.baseOffset)
      // 异步删除
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   * @throws KafkaStorageException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    // 改后缀名，在后面加上.deleted
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    // 定义一个删除方法，用于在定时任务中执行
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      segment.delete()
    }
    // 添加定时任务删除文件
    scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    // 加锁
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      // 修改.cleaned -> .swap
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)

      // 将新的LogSegment添加到Log.segments跳表
      addSegment(newSegment)

      // delete the old files
      // 删除旧的LogSegment
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
          // 删除LogSegment
          segments.remove(seg.baseOffset)
        // delete segment
        // 删除文件
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      // 移除.swap文件的.swap后缀
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
    * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
   * Construct an index file name in the given dir using the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)


  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicAndPartition = {
    // 获取目录名称
    val name: String = dir.getName
    if (name == null || name.isEmpty || !name.contains('-')) {
      throwException(dir)
    }
    // 取出"-"的位置
    val index = name.lastIndexOf('-')
    // "-"前面是topic名称
    val topic: String = name.substring(0, index)
    // "-"后面是partition名称
    val partition: String = name.substring(index + 1)
    if (topic.length < 1 || partition.length < 1) {
      throwException(dir)
    }
    // 构成TopicAndPartition对象
    TopicAndPartition(topic, partition.toInt)
  }

  def throwException(dir: File) {
    throw new KafkaException("Found directory " + dir.getCanonicalPath + ", " +
      "'" + dir.getName + "' is not in the form of topic-partition\n" +
      "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
      "directory")
  }
}

