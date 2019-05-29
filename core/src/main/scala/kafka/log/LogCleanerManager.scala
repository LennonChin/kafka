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

import java.io.File
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.utils.{Logging, Pool}
import kafka.server.OffsetCheckpoint
import collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils._
import java.util.concurrent.TimeUnit
import kafka.common.{LogCleaningAbortedException, TopicAndPartition}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState // 刚开始进入压缩任务
private[log] case object LogCleaningAborted extends LogCleaningState // 压缩任务被中断
private[log] case object LogCleaningPaused extends LogCleaningState // 压缩任务被暂停

/**
 *  Manage the state of each partition being cleaned.
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 */
private[log] class LogCleanerManager(val logDirs: Array[File], val logs: Pool[TopicAndPartition, Log]) extends Logging with KafkaMetricsGroup {
  
  override val loggerName = classOf[LogCleaner].getName

  // package-private for testing
  // Cleaner Checkpoint文件名
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"
  
  /**
    * the offset checkpoints holding the last cleaned point for each log
    * 用于维护data数据目录与cleaner-offset-checkpoint文件之间的对应关系，与LogManager的recoverPointCheckpoints集合类似
    * */
  private val checkpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)))).toMap

  /**
    * the set of logs currently being cleaned
    * 记录正在进行清理的TopicAndPartition的压缩状态
    * */
  private val inProgress = mutable.HashMap[TopicAndPartition, LogCleaningState]()

  /**
    * a global lock used to control all access to the in-progress set and the offset checkpoints
    * 用于保护checkpoints集合和inProgress集合的锁
    * */
  private val lock = new ReentrantLock
  
  /**
    * for coordinating the pausing and the cleaning of a partition
    * 用于线程阻塞等待压缩状态由LogCleaningAborted转换为LogCleaningPaused
    * */
  private val pausedCleaningCond = lock.newCondition()
  
  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints(): Map[TopicAndPartition, Long] =
    // 将所有的checkpoint读取出来
    checkpoints.values.flatMap(_.read()).toMap

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
     * 选取下一个需要进行日志压缩的Log
     * filthiest
     * 英 [ˈfɪlθɪɪst]，美 [ˈfɪlθɪɪst]
     * adv. 极其肮脏的;富得流油的
     * adj. 肮脏的;污秽的;下流的;淫秽的;猥亵的;气愤的
     * filthy的最高级
    */
  def grabFilthiestLog(): Option[LogToClean] = {
    // 加锁
    inLock(lock) {
      // 获取全部Log的cleaner checkpoint
      val lastClean = allCleanerCheckpoints()
      val dirtyLogs = logs.filter {
        // 过滤掉cleanup.policy为delete的Log
        case (topicAndPartition, log) => log.config.compact  // skip any logs marked for delete rather than dedupe
      }.filterNot {
        // 过滤掉inProgress中的Log
        case (topicAndPartition, log) => inProgress.contains(topicAndPartition) // skip any logs already in-progress
      }.map {
        case (topicAndPartition, log) => // create a LogToClean instance for each
          // if the log segments are abnormally truncated and hence the checkpointed offset
          // is no longer valid, reset to the log starting offset and log the error event
          // 获取Log中第一条消息的offset
          val logStartOffset = log.logSegments.head.baseOffset
          // 决定最终的压缩开始的位置，firstDirtyOffset的值可能是logStartOffset，也可能是clean checkpoint
          val firstDirtyOffset = {
            val offset = lastClean.getOrElse(topicAndPartition, logStartOffset)
            if (offset < logStartOffset) {
              error("Resetting first dirty offset to log start offset %d since the checkpointed offset %d is invalid."
                    .format(logStartOffset, offset))
              logStartOffset
            } else {
              offset
            }
          }
          // 为每个Log创建一个LogToClean对象，该对象内部维护了每个Log的clean部分字节数、dirty部分字节数以及cleanableRatio
          LogToClean(topicAndPartition, log, firstDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs 过滤掉空Log

      // 获取dirtyLogs集合中cleanableRatio的最大值
      this.dirtiestLogCleanableRatio = if (!dirtyLogs.isEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio
      // 过滤掉cleanableRatio小于配置的minCleanableRatio值的Log
      val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      if(cleanableLogs.isEmpty) {
        None
      } else {
        // 选择要压缩的Log，只会选出最大的那个，内部是通过LogToClean对象的cleanableRatio值来比较
        val filthiest = cleanableLogs.max
        // 更新（或添加）此分区对应的压缩状态，将压缩状态置为LogCleaningInProgress
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        // 返回要压缩的日志对应的LogToClean对象
        Some(filthiest)
      }
    }
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicAndPartition)
      resumeCleaning(topicAndPartition)
    }
    info("The cleaning for partition %s is aborted".format(topicAndPartition))
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              inProgress.put(topicAndPartition, LogCleaningAborted)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be aborted and paused since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
      while (!isCleaningInState(topicAndPartition, LogCleaningPaused))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
    info("The cleaning for partition %s is aborted and paused".format(topicAndPartition))
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is not paused."
                                          .format(topicAndPartition))
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicAndPartition)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
    }
    info("Compaction for partition %s is resumed".format(topicAndPartition))
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInState(topicAndPartition: TopicAndPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicAndPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
    *  检查清理状态是否为LogCleaningAborted，如果是会抛出LogCleaningAbortedException异常
   */
  def checkCleaningAborted(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      if (isCleaningInState(topicAndPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  // 用于修改cleaner-offset-checkpoint
  def updateCheckpoints(dataDir: File, update: Option[(TopicAndPartition,Long)]) {
    // 加锁
    inLock(lock) {
      // 获取指定Log目录对应的cleaner-offset-checkpoint文件
      val checkpoint = checkpoints(dataDir) // 得到OffsetCheckpoint对象
      /**
        * 对相同的key的value进行覆盖
        * 该方法会使用update元组中键值对覆盖logs.keys中已存在的键值对
        * read方法读取的结果为Map[TopicAndPartition, Long]字典
        */
      val existing = checkpoint.read().filterKeys(logs.keys) ++ update
      // 更新cleaner-offset-checkpoint文件，重新写checkpoint文件
      checkpoint.write(existing)
    }
  }

  def maybeTruncateCheckpoint(dataDir: File, topicAndPartition: TopicAndPartition, offset: Long) {
    inLock(lock) {
      if (logs.get(topicAndPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        val existing = checkpoint.read()

        if (existing.getOrElse(topicAndPartition, 0L) > offset)
          checkpoint.write(existing + (topicAndPartition -> offset))
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicAndPartition: TopicAndPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress(topicAndPartition) match {
        case LogCleaningInProgress =>
          updateCheckpoints(dataDir,Option(topicAndPartition, endOffset))
          inProgress.remove(topicAndPartition)
        case LogCleaningAborted =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case s =>
          throw new IllegalStateException("In-progress partition %s cannot be in %s state.".format(topicAndPartition, s))
      }
    }
  }
}
