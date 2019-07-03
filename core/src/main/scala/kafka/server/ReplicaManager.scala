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
package kafka.server

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogManager}
import kafka.message.{ByteBufferMessageSet, InvalidMessageException, Message, MessageSet}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.errors.{ControllerMovedException, CorruptRecordException, InvalidTimestampException,
                                        InvalidTopicException, NotLeaderForPartitionException, OffsetOutOfRangeException,
                                        RecordBatchTooLargeException, RecordTooLargeException, ReplicaNotAvailableException,
                                        UnknownTopicOrPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, PartitionState, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time => JTime}

import scala.collection._
import scala.collection.JavaConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, error: Option[Throwable] = None) {
  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         hw: Long,
                         readSize: Int,
                         isReadFromLogEnd : Boolean,
                         error: Option[Throwable] = None) {

  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }

  override def toString = {
    "Fetch Data: [%s], HW: [%d], readSize: [%d], isReadFromLogEnd: [%b], error: [%s]"
            .format(info, hw, readSize, isReadFromLogEnd, error)
  }
}

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata,
                                                         MessageSet.Empty),
                                           -1L,
                                           -1,
                                           false)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Short], errorCode: Short) {

  override def toString = {
    "update results: [%s], global error: [%d]".format(responseMap, errorCode)
  }
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

/**
  * @param config
  * @param metrics
  * @param time
  * @param jTime
  * @param zkUtils 操作Zookeeper的辅助对象
  * @param scheduler KafkaScheduler调度器
  * @param logManager LogManager对象，用于对日志系统进行读写
  * @param isShuttingDown
  * @param threadNamePrefix
  */
class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     jTime: JTime,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  // KafkaController年代信息，在重新选举Controller Leader时该字段值会递增
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  // 当前broker的ID，用于查找本地副本
  private val localBrokerId = config.brokerId
  // 保存了当前broker上分配的所有Partition信息
  private val allPartitions = new Pool[(String, Int), Partition](valueFactory = Some { case (t, p) =>
    // 当找不到键对应的Partition时会使用该valueFactory创建一个并放入Pool中
    new Partition(t, p, time, this)
  })
  private val replicaStateChangeLock = new Object
  /**
    * ReplicaFetcherManager中管理了多个ReplicaFetchThread线程，
    * ReplicaFetchThread线程会向Leader副本发送FetchRequest请求来同步Follower副本的消息数据
    */
  val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, jTime, threadNamePrefix)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  // 存储了每个数据目录及映射的HighWatermark Checkpoint文件，HighWatermark Checkpoint文件记录了每个分区的HighWatermark线
  val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger
  // 用于记录ISR集合发生变化的分区信息
  private val isrChangeSet: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())
  // 生产请求的DelayedOperationPurgatory
  val delayedProducePurgatory = DelayedOperationPurgatory[DelayedProduce](
    purgatoryName = "Produce", config.brokerId, config.producerPurgatoryPurgeIntervalRequests)
  // 拉取请求的DelayedOperationPurgatory
  val delayedFetchPurgatory = DelayedOperationPurgatory[DelayedFetch](
    purgatoryName = "Fetch", config.brokerId, config.fetchPurgatoryPurgeIntervalRequests)

  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
          getLeaderPartitions().size
      }
    }
  )
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount()
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)

  def underReplicatedPartitionCount(): Int = {
      getLeaderPartitions().count(_.isUnderReplicated)
  }

  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true)) // 检测HighWatermark Checkpoint任务是否启动
      // 启动HighWatermark Checkpoint任务
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicAndPartition: TopicAndPartition) {
    isrChangeSet synchronized { // 加锁
      // 添加到isrChangeSet
      isrChangeSet += topicAndPartition
      // 记录最后的更新时间
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }

  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized { // 加锁
      /**
        * 检查是否需要执行定时任务：1 & (2 || 3)
        * 1. isrChangeSet不为空；
        * 2. 最后一次有ISR集合发生变化的时间距今已超过5秒；
        * 3. 上次写入Zookeeper的时间距今已超过60秒。
        */
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        // 将isrChangeSet写入Zookeeper
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
        // 清空isrChangeSet
        isrChangeSet.clear()
        // 更新lastIsrPropagationMs
        lastIsrPropagationMs.set(now)
      }
    }
  }

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    // 尝试完成DelayedProduce炼狱中的DelayedProduce延迟任务
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    // 使用DelayedOperationPurgatory来完成延迟操作
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  def startup() {
    // start ISR expiration thread
    // ISR Expiration定时任务
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
    // ISR Change Propagation定时任务
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica (delete=%s) for partition [%s,%d]".format(localBrokerId,
      deletePartition.toString, topic, partitionId))
    // 先记录错误码为NONE
    val errorCode = Errors.NONE.code
    // 从allPartitions获取分区进行操作
    getPartition(topic, partitionId) match {
      // 有对应的分区
      case Some(partition) =>
        if(deletePartition) { // deletePartition标识了是否需要删除分区，会删除分区对应的副本及其Log
          // 从allPartitions中移除对应分区
          val removedPartition = allPartitions.remove((topic, partitionId))
          if (removedPartition != null) {
            // 删除副本
            removedPartition.delete() // this will delete the local log
            val topicHasPartitions = allPartitions.keys.exists { case (t, _) => topic == t }
            if (!topicHasPartitions)
                BrokerTopicStats.removeMetrics(topic)
          }
        }
      // 没有对应的分区，可以直接删除Log
      case None =>
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if(deletePartition) {
          val topicAndPartition = TopicAndPartition(topic, partitionId)
          // 使用LogManager中删除对应的Log
          if(logManager.getLog(topicAndPartition).isDefined) {
              logManager.deleteLog(topicAndPartition)
          }
        }
        stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition [%s,%d] as replica doesn't exist on broker"
          .format(localBrokerId, deletePartition, topic, partitionId))
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition [%s,%d]"
      .format(localBrokerId, deletePartition, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
    replicaStateChangeLock synchronized { // 加锁
      // 构造字典，用于存放每个分区的响应结果
      val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {  // 检查Controller的年代信息是否合法
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
        // 返回年代信息过期的错误
        (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        // 从请求中得到操作的分区
        val partitions = stopReplicaRequest.partitions.asScala
        // 更新年代信息
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        // 停止对指定分区的拉取操作
        replicaFetcherManager.removeFetcherForPartitions(partitions.map(r => TopicAndPartition(r.topic, r.partition)))
        for(topicPartition <- partitions){
          // 使用stopReplica()方法关闭指定的分区的副本
          val errorCode = stopReplica(topicPartition.topic, topicPartition.partition, stopReplicaRequest.deletePartitions)
          // 向responseMap记录错误码
          responseMap.put(topicPartition, errorCode)
        }
        (responseMap, Errors.NONE.code)
      }
    }
  }

  def getOrCreatePartition(topic: String, partitionId: Int): Partition = {
    // 使用的是Pool池的方法
    allPartitions.getAndMaybePut((topic, partitionId))
  }

  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    // 获取对应主题分区的Replica对象，无论是否是Leader分区
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    // 获取主题分区对应的Partition对象
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      // 无法匹配对应的主题分区，抛出UnknownTopicOrPartitionException异常
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      // 可以匹配对应的主题分区
      case Some(partition) =>
        // 获得对应的Leader分区Replica对象
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                                                     .format(topic, partitionId, config.brokerId))
        }
    }
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied
   */
  def appendMessages(timeout: Long,
                     requiredAcks: Short,
                     internalTopicsAllowed: Boolean,
                     messagesPerPartition: Map[TopicPartition, MessageSet],
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {

    if (isValidRequiredAcks(requiredAcks)) { // 检查ACK参数是否合法-1、0或1中的一个
      val sTime = SystemTime.milliseconds
      // 将消息追加到Log中，同时还会检测delayedFetchPurgatory中相关key对应的DelayedFetch，满足条件则将其执行完成
      val localProduceResults = appendToLocalLog(internalTopicsAllowed, messagesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))
      // 对追加结果进行转换，注意ProducePartitionStatus的参数
      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.errorCode, result.info.firstOffset, result.info.timestamp)) // response status
      }
      // 检测是否生成DelayedProduce，其中一个条件是检测ProduceRequest中的acks字段是否为-1
      if (delayedRequestRequired(requiredAcks, messagesPerPartition, localProduceResults)) {
        /**
          * 走到这里说明delayedRequestRequired()方法返回值为true，需要满足下面三个条件：
          * 1. ACK为-1；
          * 2. 有数据需要写入；
          * 3. 至少有一个分区的数据写入是成功的。
          */
        // create delayed produce operation
        // 创建DelayedProduce对象
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        // 将ProduceRequest中的主题映射为TopicPartitionOperationKey序列
        val producerRequestKeys = messagesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        // 尝试完成DelayedProduce，否则将DelayedProduce添加到delayedProducePurgatory中管理
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      // ACK参数不合法，返回错误信息，错误码为INVALID_REQUIRED_ACKS
      val responseStatus = messagesPerPartition.map {
        case (topicAndPartition, messageSet) =>
          (topicAndPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS.code,
                                                      LogAppendInfo.UnknownLogAppendInfo.firstOffset,
                                                      Message.NoTimestamp))
      }
      responseCallback(responseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedRequestRequired(requiredAcks: Short, messagesPerPartition: Map[TopicPartition, MessageSet],
                                       localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 && // ACK为-1
    messagesPerPartition.size > 0 && // 有消息数据需要写入
    localProduceResults.values.count(_.error.isDefined) < messagesPerPartition.size // 至少有一个分区的写入操作时成功的
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               messagesPerPartition: Map[TopicPartition, MessageSet],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(messagesPerPartition))
    // 遍历消息集合
    messagesPerPartition.map { case (topicPartition, messages) =>
      BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        // 内部主题且内部主题不允许添加，拒绝添加
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException("Cannot append to internal topic %s".format(topicPartition.topic)))
        ))
      } else {
        try {
          // 根据主题和分区获取对应的Partition对象
          val partitionOpt = getPartition(topicPartition.topic, topicPartition.partition)
          val info = partitionOpt match {
            // 分区存在
            case Some(partition) =>
              // 通过Partition对象将消息添加到Leader副本分区，该方法还会尝试在HighWatermark发生更新时尝试完成写入消息和拉取消息的请求
              partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet], requiredAcks)
            // 分区不存在，抛出UnknownTopicOrPartitionException异常
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicPartition, localBrokerId))
          }

          // 添加的消息的数量
          val numAppendedMessages =
            if (info.firstOffset == -1L || info.lastOffset == -1L)
              0
            else
              info.lastOffset - info.firstOffset + 1

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
            .format(messages.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
          // 返回字典结果
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e: KafkaStorageException =>
            fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
            Runtime.getRuntime.halt(1)
            (topicPartition, null)
          // 将异常包装后向上抛出，这里包括了NotLeaderForPartitionException异常
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException | // 当前分区不是Leader分区的异常
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: InvalidMessageException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).failedProduceRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchInfo: immutable.Map[TopicAndPartition, PartitionFetchInfo],
                    responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit) {
    // 只有Follower副本才有replicaId，消费者是没有的，消费者的replicaId始终是-1
    val isFromFollower = replicaId >= 0
    // 如果replicaId != -2，就说明是正常的拉取请求，则只能拉取Leader副本的数据
    val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
    // 判断是否是从broker发出的拉取请求，不是的话就只能读取HighWatermark线以下的数据
    val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

    // read from local logs
    // 从Log中读取消息
    val logReadResults = readFromLocalLog(fetchOnlyFromLeader, fetchOnlyCommitted, fetchInfo)

    // if the fetch comes from the follower,
    // update its corresponding log end offset
    // 检测FetchRequest是否来自Follower副本
    if(Request.isValidBrokerId(replicaId))
      /**
        * updateFollowerLogReadResults()方法用来处理来自Follower副本的FetchRequest请求，主要做下面4件事：
        * 1. 在Leader中维护了Follower副本的各种状态，这里会更新对应Follower副本的状态，例如，更新LEO、更新lastCaughtUpTimeMsUnderlying等；
        * 2. 检测是否需要对ISR集合进行扩张，如果ISR集合发生变化，则将新的ISR集合的记录保存到ZooKeeper中；
        * 3. 检测是否后移Leader的HighWatermark；
        * 4. 检测delayedProducePurgatory中相关key对应的DelayedProduce，满足条件则将其执行完成。
        */
      updateFollowerLogReadResults(replicaId, logReadResults)

    // check if this fetch request can be satisfied right away
    // 统计从Log中读取的字节总数
    val bytesReadable = logReadResults.values.map(_.info.messageSet.sizeInBytes).sum
    // 统计在从Log中读取消息的时候，是否发生了异常
    val errorReadingData = logReadResults.values.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.errorCode != Errors.NONE.code))

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    /**
      * 判断是否能够立即返回FetchResponse，下面四个条件满足任意一个就可以立即返回FetchResponse：
      * 1. FetchRequest的timeout＜=0，即消费者或Follower副本不希望等待；
      * 2. FetchRequest没有指定要读取的分区，即fetchInfo.size ＜= 0；
      * 3. 已经读取了足够的数据，即bytesReadable ＞= fetchMinBytes；
      * 4. 在读取数据的过程中发生了异常，即检查errorReadingData。
      */
    if(timeout <= 0 || fetchInfo.size <= 0 || bytesReadable >= fetchMinBytes || errorReadingData) {
      val fetchPartitionData = logReadResults.mapValues(result =>
        FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))
      // 直接调用回调函数，生成并发送FetchResponse
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      // 对读取Log的结果进行转换
      val fetchPartitionStatus = logReadResults.map { case (topicAndPartition, result) =>
        (topicAndPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo.get(topicAndPartition).get))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchOnlyFromLeader, fetchOnlyCommitted, isFromFollower, fetchPartitionStatus)
      // 创建DelayedFetch对象
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      // 创建delayedFetchKeys
      val delayedFetchKeys = fetchPartitionStatus.keys.map(new TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      // 完成DelayedFetch，否则将DelayedFetch添加到delayedFetchPurgatory中管理
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
    * Read from a single topic/partition at the given offset upto maxSize bytes
    * 从指定的主题分区读取数据，PartitionFetchInfo对象中存储了读取的起始offset和字节大小
    * @param fetchOnlyFromLeader 是否只读取Leader副本的消息，只有处于debug状态下该参数才会为false
    * @param readOnlyCommitted 表示是否只读取完成提交的消息（即HW之前的消息），如果是处理来自消费者的请求此参数为true， Follower副本对应false
    * @param readPartitionInfo 记录了每个分区读取的起始offset位置和最大字节数
    * @return
    */
  def readFromLocalLog(fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       readPartitionInfo: Map[TopicAndPartition, PartitionFetchInfo]): Map[TopicAndPartition, LogReadResult] = {

    readPartitionInfo.map { case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
      BrokerTopicStats.getBrokerTopicStats(topic).totalFetchRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.mark()

      val partitionDataAndOffsetInfo =
        try {
          trace("Fetching log segment for topic %s, partition %d, offset %d, size %d".format(topic, partition, offset, fetchSize))

          // decide whether to only fetch from leader
          // 根据fetchOnlyFromLeader参数决定是否只从Leader分区拉取
          val localReplica = if (fetchOnlyFromLeader)
            // 该方法会获取Leader分区的Replica对象
            getLeaderReplicaIfLocal(topic, partition)
          else
            // 该方法获取的Replica对象不保证是Leader分区的
            getReplicaOrException(topic, partition)

          // decide whether to only fetch committed data (i.e. messages below high watermark)
          // 根据readOnlyCommitted参数决定是否只拉取HighWatermark线以下的数据
          val maxOffsetOpt = if (readOnlyCommitted)
            Some(localReplica.highWatermark.messageOffset)
          else
            None

          /* Read the LogOffsetMetadata prior to performing the read from the log.
           * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
           * Using the log end offset after performing the read can lead to a race condition
           * where data gets appended to the log immediately after the replica has consumed from it
           * This can cause a replica to always be out of sync.
           */
          // 获取Replica的logEndOffset
          val initialLogEndOffset = localReplica.logEndOffset
          val logReadInfo = localReplica.log match {
            case Some(log) =>
              // 从Replica的Log中读取数据，注意此时读取的最大offset由maxOffsetOpt控制了
              log.read(offset, fetchSize, maxOffsetOpt)
            case None =>
              error("Leader for partition [%s,%d] does not have a local log".format(topic, partition))
              FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty)
          }
          // 检查是否读到了Log的结尾
          val readToEndOfLog = initialLogEndOffset.messageOffset - logReadInfo.fetchOffsetMetadata.messageOffset <= 0
          // 将读取到的数据构造为LogReadResult对象
          LogReadResult(logReadInfo, localReplica.highWatermark.messageOffset, fetchSize, readToEndOfLog, None)
        } catch {
          // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
          // is supposed to indicate un-expected failure of a broker in handling a fetch request
          case utpe: UnknownTopicOrPartitionException => // 未知主题分区异常
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(utpe))
          case nle: NotLeaderForPartitionException => // 当前副本不是Leader副本异常
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(nle))
          case rnae: ReplicaNotAvailableException => // 副本不可用异常
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(rnae))
          case oor : OffsetOutOfRangeException => // 读取数据超限异常
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(oor))
          case e: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark()
            error("Error processing fetch operation on partition [%s,%d] offset %d".format(topic, partition, offset), e)
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(e))
        }
      (TopicAndPartition(topic, partition), partitionDataAndOffsetInfo)
    }
  }

  def getMessageFormatVersion(topicAndPartition: TopicAndPartition): Option[Byte] =
    // 获取分区对应的副本
    getReplica(topicAndPartition.topic, topicAndPartition.partition).flatMap { replica =>
      // 使用副本对应的Log对象中存储的配置信息获取消息的格式化版本（也即是魔数版本）
      replica.log.map(_.config.messageFormatVersion.messageFormatVersion)
    }

  // 可能需要更新Broker的元数据
  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest, metadataCache: MetadataCache) {
    replicaStateChangeLock synchronized { // 加锁
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) { // 检查Controller年代信息
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        // 抛出年代信息过期的异常
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        // 使用MetadataCache更新
        metadataCache.updateCache(correlationId, updateMetadataRequest)
        // 更新Controller年代信息
        controllerEpoch = updateMetadataRequest.controllerEpoch
      }
    }
  }

  // 负责副本角色切换
  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndISRRequest: LeaderAndIsrRequest,
                             metadataCache: MetadataCache,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
    // 记录日志
    leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
    }
    replicaStateChangeLock synchronized { // 加锁
      // 新建一个HashMap用于记录产生的错误码
      val responseMap = new mutable.HashMap[TopicPartition, Short]
      if (leaderAndISRRequest.controllerEpoch < controllerEpoch) { // 检查请求中的Controller的年代信息
        // 如果请求中的Controller年代信息小于当前的年代信息，直接返回STALE_CONTROLLER_EPOCH异常码
        // 日志处理
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        }
        // 构造BecomeLeaderOrFollowerResult对象用于返回，错误码为STALE_CONTROLLER_EPOCH
        BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        // 年代信息符合要求
        // 记录controllerId
        val controllerId = leaderAndISRRequest.controllerId
        // 更新Controller的年代信息
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        // 构造一个HashMap用于记录对应的分区状态
        val partitionState = new mutable.HashMap[Partition, PartitionState]()
        // 遍历请求中的partitionStates
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
          // 获取对应主题分区的Partition对象，如果没有就创建
          val partition = getOrCreatePartition(topicPartition.topic, topicPartition.partition)
          // 获取Partition的Leader副本年代信息
          val partitionLeaderEpoch = partition.getLeaderEpoch()
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
          if (partitionLeaderEpoch < stateInfo.leaderEpoch) { // 判断当前Partition的年代信息是否合法
            // 判断该分配方案中是否有副本被分配到了当前的broker
            if(stateInfo.replicas.contains(config.brokerId))
              // 保留与当前broker相关的Partition以及PartitionState
              partitionState.put(partition, stateInfo)
            else {
              // 否则记录日志并在responseMap中记录UNKNOWN_TOPIC_OR_PARTITION错误码
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                  topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            }
          } else {
            // Otherwise record the error code in response
            // 当前Partition的年代信息不合法，记录日志，在responseMap中记录STALE_CONTROLLER_EPOCH错误码
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is old. Current leader epoch is %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
          }
        }

        // 根据PartitionState中指定的角色进行分类
        val partitionsTobeLeader = partitionState.filter { case (partition, stateInfo) =>
          stateInfo.leader == config.brokerId
        }
        val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys)

        // 根据分配结果切换Leader和Follower副本
        val partitionsBecomeLeader = if (!partitionsTobeLeader.isEmpty)
          // 切换Leader副本
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (!partitionsToBeFollower.isEmpty)
          // 切换Follower副本
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
        else
          Set.empty[Partition]

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        // 启动HighWatermark Checkpoint任务
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()

        // 执行onLeadershipChange回调函数
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        // 构造BecomeLeaderOrFollowerResult结果对象，错误码为NONE
        BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {
    // 记录日志
    partitionState.foreach(state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))

    // 初始化每个分区的错误码为NONE
    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

    try {
      // First stop fetchers for all the partitions
      // 在此broker上的副本之前可能是Follower，需要先暂停它们的拉取操作
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      // Update the partition information to be the leader
      // 更新即将成为Leader的Partition信息
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        // 使用Partition的makeLeader(...)方法将分区的本地副本切换为Leader
        if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
          // 记录成功从其他状态（第一次启动或Follower副本）切换成Leader副本的分区
          partitionsToMakeLeaders += partition
        else
          // 日志记录，Leader所在的broker没有发生变化
          stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is already the leader for the partition.")
            .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(partition.topic, partition.partitionId)));
      }
      partitionsToMakeLeaders.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }
    } catch {
      case e: Throwable =>
        partitionState.foreach { state =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
                                                TopicAndPartition(state._1.topic, state._1.partitionId))
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }
    // 返回切换成Leader的副本
    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short],
                            metadataCache: MetadataCache) : Set[Partition] = {
    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    // 初始化每个分区对应的错误码为NONE
    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    // 用于记录切换为Follower副本的集合
    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        // 检测新的Leader所在的broker是否存货
        // 获取Leader副本所在broker节点的ID
        val newLeaderBrokerId = partitionStateInfo.leader
        // 从元数据中获取存活的broker集合，然后在其中查找Leader副本所在Broker的ID
        metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
          // Only change partition state when the leader is available
          case Some(leaderBroker) => // 找到了
            // 调用Partition的makeFollower(...)方法将Partition的本地副本切换为Follower副本
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
              // 记录成功从其他状态（第一次启动或Leader）切换到Follower副本的分区
              partitionsToMakeFollower += partition
            else
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition [%s,%d] since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                partition.topic, partition.partitionId, newLeaderBrokerId))
          case None => // 没找到
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
              " %d epoch %d for partition [%s,%d] but cannot become follower since the new leader %d is unavailable.")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
              partition.topic, partition.partitionId, newLeaderBrokerId))
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            // 即使Leader副本所在的broker不可用，也需要创建Local Replica
            partition.getOrCreateReplica()
        }
      }

      // 停止与旧Leader同步的拉取线程
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)))

      // 日志记录
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }

      /**
        * 由于Leader副本已发生变化，所以新旧Leader副本在HighWatermark ~ LogEndOffset之间的消息可能是不一致的，
        * 但HighWatermark之前的消息是一致的，所以将Log截断到HighWatermark。
        */
      logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark.messageOffset)).toMap)

      // 尝试完成与切换为Follower副本的分区相关的DelayedOperation延迟任务
      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topic, partition.partitionId)
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition [%s,%d] as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topic, partition.partitionId, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) { // 检测ReplicaManager的运行状态
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topic, partition.partitionId))
        }
      }
      else {
        // 重新开启与新Leader副本同步的拉取线程
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        // 将partitionsToMakeFollower映射为TopicAndPartition -> BrokerAndInitialOffset的字典
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          // TopicAndPartition -> BrokerAndInitialOffset的键值对
          new TopicAndPartition(partition) -> BrokerAndInitialOffset(
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerSecurityProtocol),
            partition.getReplica().get.logEndOffset.messageOffset)).toMap
        // 添加拉取任务
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition [%s,%d]")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    // 尝试对ISR进行紧缩操作，使用的是Partition的maybeShrinkIsr()方法
    allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  private def updateFollowerLogReadResults(replicaId: Int, readResults: Map[TopicAndPartition, LogReadResult]) {
    debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
    // 遍历拉取的数据结果
    readResults.foreach { case (topicAndPartition, readResult) =>
      // 获取对应的主题分区的Partition对象
      getPartition(topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(partition) =>
          // 更新副本
          partition.updateReplicaLogReadResult(replicaId, readResult)

          // for producer requests with ack > 1, we need to check
          // if they can be unblocked after some follower's log end offsets have moved
          /**
            * 尝试完成时间轮中的DelayedProduce延迟任务；因为在生产消息时可能需要等待In-Sync副本同步才能向客户端确认ACK
            * 如果Follower副本完成了同步，理应主动触发在向同一个主题分区写数据而生成的DelayedProduce延迟任务，尝试完成生产请求
            */
          tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicAndPartition))
        case None =>
          warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicAndPartition))
      }
    }
  }

  private def getLeaderPartitions() : List[Partition] = {
    allPartitions.values.filter(_.leaderReplicaIfLocal().isDefined).toList
  }

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks() {
    // 获取全部的Replica对象，按照副本所在的数据目录进行分组
    val replicas = allPartitions.values.flatMap(_.getReplica(config.brokerId))
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
    // 遍历所有的数据目录
    for ((dir, reps) <- replicasByDir) {
      // 收集当前数据目录下的全部副本的HighWatermark
      val hwms = reps.map(r => new TopicAndPartition(r) -> r.highWatermark.messageOffset).toMap
      try {
        // 更新对应数据目录下的replication-offset-checkpoint文件
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    replicaFetcherManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    if (checkpointHW)
      // 在关闭时会需要进行一次HighWatermark的Checkpoint
      checkpointHighWatermarks()
    info("Shut down completely")
  }
}
