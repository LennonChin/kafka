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

import java.io._
import java.util.concurrent.TimeUnit

import kafka.utils._

import scala.collection._
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.{BrokerState, OffsetCheckpoint, RecoveringFromUncleanShutdown}
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
  *
  * @param logDirs log目录集合，在server.properties配置文件中指定的
  * @param topicConfigs 主题配置
  * @param defaultConfig 默认配置
  * @param cleanerConfig 日志压缩的配置
  * @param ioThreads 为完成Log加载的相关操作，每个log目录下分配指定的线程执行加载
  * @param flushCheckMs kafka-log-flusher定时任务的周期时间
  * @param flushCheckpointMs kafka-recovery-point-checkpoint定时任务的周期时间
  * @param retentionCheckMs kafka-log-retention定时任务的周期时间
  * @param scheduler KafkaScheduler对象，用于执行周期任务的线程池
  * @param brokerState Broker状态
  * @param time
 */
@threadsafe
class LogManager(val logDirs: Array[File],
                 val topicConfigs: Map[String, LogConfig],
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 private val time: Time) extends Logging {
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  // 定时任务的延迟时间
  val InitialTaskDelayMs = 30*1000
  // 创建或删除Log时需要加锁进行同步
  private val logCreationOrDeletionLock = new Object
  // 用于管理TopicAndPartition与Log之间的对应关系，底层使用ConcurrentHashMap实现
  private val logs = new Pool[TopicAndPartition, Log]()

  // 保证每个log目录都存在且可读
  createAndValidateLogDirs(logDirs)
  // FileLock集合
  private val dirLocks = lockLogDirs(logDirs)
  /**
    * 用于管理每个log目录与其下的RecoveryPointCheckpoint文件之间的映射关系，
    * 在LogManager对象初始化时，会在每个log目录下创建一个对应的RecoveryPointCheckpoint文件。
    * 此Map的value是OffsetCheckpoint类型的对象，其中封装了对应log目录下的RecoveryPointCheckpoint文件，
    * 并提供对RecoveryPointCheckpoint文件的读写操作。
    * RecoveryPointCheckpoint文件中则记录了该log目录下的所有Log的recoveryPoint
    */
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  // 加载Log
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      null
  
  /**
   * Create and check validity of the given directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    /**
      * 美[kə'nɑnɪk(ə)l]英[kə'nɒnɪk(ə)l]
      * adj.被收入真经篇目的；经典的；按照基督教教会法规的
      * n.(布道时应穿的)法衣
      * 网络标准；规范的；典型
      */
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      // log目录的实际个数不准确
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    // 遍历目录
    for(dir <- dirs) {
      // 判断目录是否存在
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        // 不存在则创建目录
        val created = dir.mkdirs()
        if(!created)
          // 如果创建失败则抛出异常
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      // 保证是目录且可读
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
    * 对目录加锁，实现方式是在目录下创建一个.lock文件，然后对其Channel进行加锁
    * 如果.lock文件的Channel被加锁了，说明该目录被锁了
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      // 创建一个.lock后缀结尾的文件，并以该文件创建一个FileLock
      val lock = new FileLock(new File(dir, LockFile))
      // 对该FileLock尝试进行加锁
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath +
                               ". A Kafka instance in another process or thread is using this directory.")
      // 返回FileLock
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    // 用于保存log目录对应的线程池
    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // 遍历log目录，对每个log目录进行操作
    for (dir <- this.logDirs) {
      // 创建线程池，线程个数为ioThreads
      val pool = Executors.newFixedThreadPool(ioThreads)
      // 将线程池添加到threadPools进行记录
      threadPools.append(pool)

      /**
        * 检测broker上次是否是正常关闭的
        * 如果是正常关闭的，在目录下会保存有一个.kafka_cleanshutdown的文件
        * 如果该文件不存在，说明broker上次是非正常关闭的
        */
      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

      if (cleanShutdownFile.exists) {
        // 正常关闭
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        // 非正常关闭，修改brokerState为RecoveringFromUncleanShutdown
        brokerState.newState(RecoveringFromUncleanShutdown)
      }

      // 读取每个log目录下的RecoveryPointCheckpoint文件并生成TopicAndPartition与recoveryPoint的对应关闭
      var recoveryPoints = Map[TopicAndPartition, Long]()
      try {
        // 载入recoveryPoints
        recoveryPoints = this.recoveryPointCheckpoints(dir).read
      } catch {
        case e: Exception => {
          warn("Error occured while reading recovery-point-offset-checkpoint file of directory " + dir, e)
          warn("Resetting the recovery checkpoint to 0")
        }
      }

      // 遍历所有的log目录的子文件，将文件过滤，只保留目录
      val jobsForDir = for { // 这个括号内是遍历条件
        dirContent <- Option(dir.listFiles).toList
        logDir <- dirContent if logDir.isDirectory
      } yield { // yield代码块是for的循环体
        // 为每个log目录创建一个Runnable任务
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")
          // 从目录名解析出Topic名称和分区编号
          val topicPartition = Log.parseTopicPartitionName(logDir)
          // 获取Log对应的配置，也即是主题分区对应的配置
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          // 获取Log对应的recoveryPoint
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
          // 创建Log对象
          val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
          // 将Log对象保存到logs集合中，所有分区的Log成功加载完成
          val previous = this.logs.put(topicPartition, current)

          if (previous != null) {
            throw new IllegalArgumentException(
              "Duplicate log directories found: %s, %s!".format(
              current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
          }
        }
      }
      // 将jobsForDir中的所有任务放到线程池中执行，并将Future形成Seq，保存到jobs中
      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      // 等待jobs中的Runnable完成
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        // 删除.kafka_cleanshutdown文件
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      // 关闭全部的线程池
      threadPools.foreach(_.shutdown())
    }

    info("Logs loading complete.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      // 启动kafka-log-retention定时任务
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs, // log.retention.check.interval.ms
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      // 启动kafka-log-flusher定时任务
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs, // log.flush.scheduler.interval.ms
                         TimeUnit.MILLISECONDS)
      // 启动kafka-recovery-point-checkpoint定时任务
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs, // log.flush.offset.checkpoint.interval.ms
                         TimeUnit.MILLISECONDS)
    }
    if(cleanerConfig.enableCleaner) // log.cleaner.enable
      // 启动LogCleaner
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionAndOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    for ((topicAndPartition, truncateOffset) <- partitionAndOffsets) {
      val log = logs.get(topicAndPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner: Boolean = (truncateOffset < log.activeSegment.baseOffset)
        if (needToStopCleaner && cleaner != null)
          cleaner.abortAndPauseCleaning(topicAndPartition)
        log.truncateTo(truncateOffset)
        if (needToStopCleaner && cleaner != null) {
          cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicAndPartition, log.activeSegment.baseOffset)
          cleaner.resumeCleaning(topicAndPartition)
        }
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicAndPartition: TopicAndPartition, newOffset: Long) {
    val log = logs.get(topicAndPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicAndPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicAndPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicAndPartition)
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointRecoveryPointOffsets() {
    // 对所有的logDirs都调用checkpointLogsInDir方法
    this.logDirs.foreach(checkpointLogsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   */
  private def checkpointLogsInDir(dir: File): Unit = {
    // 获取指定log目录下的TopicAndPartition信息，以及对应的Log对象
    val recoveryPoints = this.logsByDir.get(dir.toString)
    if (recoveryPoints.isDefined) { // recoveryPoints不为空
      // 更新指定log目录下RecoveryPointCheckpoint文件
      this.recoveryPointCheckpoints(dir) // OffsetCheckpoint对象，其中保存了dir目录下的RecoveryPointCheckpoint文件
        .write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicAndPartition: TopicAndPartition): Option[Log] = {
    // 从logs目录中根据Topic和Partition获取Log
    val log = logs.get(topicAndPartition)
    if (log == null)
      None
    else
      Some(log)
  }

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
    * 根据指定的Topic和Partition创建Log对象，如果已存在就直接返回存在的
    * 会选取文件最少的log目录下创建
   */
  def createLog(topicAndPartition: TopicAndPartition, config: LogConfig): Log = {
    // 加锁
    logCreationOrDeletionLock synchronized {
      var log = logs.get(topicAndPartition)
      
      // check if the log has already been created in another thread
      // Log已存在，直接返回
      if(log != null)
        return log
      
      // if not, create it
      // 不存在，选择Log最少的目录
      val dataDir = nextLogDir()
      // 在选择的log目录下创建文件，文件名为"topic名称 - partition序号"
      val dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition)
      dir.mkdirs()
      // 根据文件创建Log对象
      log = new Log(dir, 
                    config,
                    recoveryPoint = 0L,
                    scheduler,
                    time)
      // 将Log放入logs池中
      logs.put(topicAndPartition, log)
      info("Created log for partition [%s,%d] in %s with properties {%s}."
           .format(topicAndPartition.topic, 
                   topicAndPartition.partition, 
                   dataDir.getAbsolutePath,
                   {import JavaConversions._; config.originals.mkString(", ")}))
      // 返回log
      log
    }
  }

  /**
   *  Delete a log.
    *  删除Log
   */
  def deleteLog(topicAndPartition: TopicAndPartition) {
    var removedLog: Log = null
    // 加锁
    logCreationOrDeletionLock synchronized {
      // 从logs集合中移除
      removedLog = logs.remove(topicAndPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        // 停止对此Log的压缩操作，阻塞等待压缩状态的改变
        cleaner.abortCleaning(topicAndPartition)
        // 更新cleaner-offset-checkpoint文件
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      // 删除相关的日志文件、索引文件及目录
      removedLog.delete()
      info("Deleted log for partition [%s,%d] in %s."
           .format(topicAndPartition.topic,
                   topicAndPartition.partition,
                   removedLog.dir.getAbsolutePath))
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {
      // 只有一个log目录，直接返回
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      // 有多个log目录
      // 计算每个log目录中的Log数量
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      var dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      // 排序，选择Log最少的log目录
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Runs through the log removing segments older than a certain age
    * 删除时间层面过期的日志文件
   */
  private def cleanupExpiredSegments(log: Log): Int = {
    // 检查配置的retention.ms是否小于0
    if (log.config.retentionMs < 0)
      return 0
    // 当前时间
    val startMs = time.milliseconds
    /**
      * 计算时间，并将删除操作交给LogSegment类处理
      * 删除条件是LogSegment的日志文件在最近一段时间（retentionMs）内没有被修改
      */
    log.deleteOldSegments(startMs - _.lastModified > log.config.retentionMs)
  }

  /**
   *  Runs through the log removing segments until the size of the log
   *  is at least logRetentionSize bytes in size
    *  删除大小超限的日志文件
   */
  private def cleanupSegmentsToMaintainSize(log: Log): Int = {
    // 检查是否配置了retention.bytes，且要求该配置大于0
    if(log.config.retentionSize < 0 || log.size < log.config.retentionSize)
      return 0
    // 根据Log的大小及retentionSize计算需要删除的字节数
    var diff = log.size - log.config.retentionSize
    // 判断该LogSegment是否应该被删除
    def shouldDelete(segment: LogSegment) = {
      // 如果需要删除的字节数大于segment的字节数，说明该LogSegment可以被删除
      if(diff - segment.size >= 0) {
        // 更新需要删除的字节数
        diff -= segment.size
        true
      } else {
        false
      }
    }
    // 调用Log的deleteOldSegments()方法筛选并删除日志文件
    log.deleteOldSegments(shouldDelete)
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
    * 按照两个条件进行LogSegment的清理工作：
    *   - LogSegment的存活时长；
    *   - 整个Log的大小。
    * log-retention任务不仅会将过期的LogSegment删除，还会根据Log的大小决定是否删除最旧的LogSegment，以控制整个Log的大小。
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    // 遍历所有Log对象，只处理cleanup.policy配置为delete的Log
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      // 使用cleanupExpiredSegments()和cleanupSegmentsToMaintainSize()方法进行清理
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicAndPartition => Log
   */
  def logsByTopicPartition: Map[TopicAndPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir = {
    /**
      * 对Map[TopicAndPartition, Log]进行目录分组聚合
      * 最终得到Map[String, Map[TopicAndPartition, Log]]结构的字典数据
      * 其中键为TopicAndPartition的父目录
      */
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")
    // 遍历logs集合
    for ((topicAndPartition, log) <- logs) {
      try {
        // 计算距离上次刷新的间隔时间
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicAndPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        // 检查是否到了需要执行flush操作的时间
        if(timeSinceLastFlush >= log.config.flushMs)
          // 调用Log的flush()方法刷新，实际刷新到LEO位置
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicAndPartition.topic, e)
      }
    }
  }
}
