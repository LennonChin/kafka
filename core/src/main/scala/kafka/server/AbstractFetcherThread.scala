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

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.message.{MessageAndOffset, ByteBufferMessageSet}
import kafka.utils.{Pool, ShutdownableThread, DelayedItem}
import kafka.common.{KafkaException, ClientIdAndBroker, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._
import scala.collection.{mutable, Set, Map}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  // 维护了TopicAndPartition与PartitionFetchState之间的对应关系，PartitionFetchState记录了对应分区的同步offset位置以及同步状态
  private val partitionMap = new mutable.HashMap[TopicAndPartition, PartitionFetchState] // a (topic, partition) -> partitionFetchState map
  // 用于保证partitionMap的重入锁
  private val partitionMapLock = new ReentrantLock
  // 重入锁的条件队列
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  // 处理拉取得到的数据
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  // 处理offset越界的情况
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  // 处理错误
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  // 创建FetchRequest
  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): REQ

  // 拉取操作
  protected def fetch(fetchRequest: REQ): Map[TopicAndPartition, PD]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork() {

    val fetchRequest = inLock(partitionMapLock) { // 加锁
      // 创建FetchRequest
      val fetchRequest = buildFetchRequest(partitionMap)
      if (fetchRequest.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        // FetchRequest为空时，退避一段时间后重试
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequest
    }

    // FetchRequest不为空，发送并处理FetchRequest请求
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  private def processFetchRequest(fetchRequest: REQ) {
    // 创建一个集合记录出错的分区
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    // 记录响应数据的字典
    var responseData: Map[TopicAndPartition, PD] = Map.empty

    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      /**
        * 发送FetchRequest并等待FetchResponse，该方法是抽象方法，交由子类实现
        * responseData用于记录返回的响应
        */
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionMap.keys
            // there is an error occurred while fetching partitions, sleep a while
            // 出现异常，退避一段时间
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) { // 处理FetchResponse
      // process fetched data
      inLock(partitionMapLock) { // 加锁
        // 遍历responseData
        responseData.foreach { case (topicAndPartition, partitionData) =>
          val TopicAndPartition(topic, partitionId) = topicAndPartition
          // 从partitionMap获取分区对应的PartitionFetchState
          partitionMap.get(topicAndPartition).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            // 响应中得到的该分区的offset与之前partitionMap中记录的一致，将进行数据写入
            if (fetchRequest.offset(topicAndPartition) == currentPartitionFetchState.offset) {
              // 处理错误码
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE => // 没有错误
                  try {
                    // 获取返回的消息集合
                    val messages = partitionData.toByteBufferMessageSet
                    // 有效的字节数
                    val validBytes = messages.validBytes
                    // 获取返回的最后一条消息的offset
                    val newOffset = messages.shallowIterator.toSeq.lastOption match {
                      case Some(m: MessageAndOffset) => m.nextOffset
                      case None => currentPartitionFetchState.offset
                    }
                    // 更新Fetch状态
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    fetcherStats.byteRate.mark(validBytes)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    // 将从Leader副本获取的消息集合追加到Log中，抽象方法，交由子类实现
                    processPartitionData(topicAndPartition, currentPartitionFetchState.offset, partitionData)
                  } catch {
                    // 异常处理
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                    case e: Throwable =>
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  // 若Follower副本请求的offset超出了Leader的LEO，则返回该错误码
                  try {
                    // 生成新的offset，handleOffsetOutOfRange()是抽象方法，交由子类实现
                    val newOffset = handleOffsetOutOfRange(topicAndPartition)
                    // 更新Fetch状态
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch {
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      partitionsWithError += topicAndPartition
                  }
                case _ =>
                  // 返回其他错误码，则进行收集后，由handlePartitionsWithErrors()方法处理
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    partitionsWithError += topicAndPartition
                  }
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      // 处理错误，抽象方法，交由子类实现
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  // 添加对应分区的拉取任务
  def addPartitions(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    partitionMapLock.lockInterruptibly() // 加锁
    try {
      // 遍历以获取分区及对应的同步起始位置
      for ((topicAndPartition, offset) <- partitionAndOffsets) {
        // If the partitionMap already has the topic/partition, then do not update the map with the old offset
        // 检测分区是否已经存在
        if (!partitionMap.contains(topicAndPartition))
          // 如果不存在，添加对应的PartitionFetchState
          partitionMap.put(
            topicAndPartition,
            // 当offset小于0时需要单独处理
            if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(topicAndPartition))
            else new PartitionFetchState(offset)
          )}
      // 唤醒当前Fetcher线程，进行同步操作
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  // 延迟某些分区的拉取操作
  def delayPartitions(partitions: Iterable[TopicAndPartition], delay: Long) {
    partitionMapLock.lockInterruptibly() // 加锁
    try {
      for (partition <- partitions) {
        // 获取分区对应的PartitionFetchState
        partitionMap.get(partition).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive) // 检测分区的同步状态
            // 将分区对应的同步状态由激活状态设置为延时状态，延迟时长为delay毫秒
            partitionMap.put(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      // 唤醒fetcher线程
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  // 移除分区对应的拉取状态
  def removePartitions(topicAndPartitions: Set[TopicAndPartition]) {
    partitionMapLock.lockInterruptibly() // 加锁
    try {
      // 遍历以从partitionMap字典中移除
      topicAndPartitions.foreach { topicAndPartition =>
        // 从partitionMap中移除
        partitionMap.remove(topicAndPartition)
        // 从fetcherLagStats中移除
        fetcherLagStats.unregister(topicAndPartition.topic, topicAndPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  // 统计分区数量
  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionMap.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicAndPartition: TopicAndPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toByteBufferMessageSet: ByteBufferMessageSet
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active , inactive)
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {

  // DelayedItem用于表示延迟时间，它实现了Delayed接口
  def this(offset: Long) = this(offset, new DelayedItem(0))

  // 是否是激活状态
  def isActive: Boolean = { delay.getDelay(TimeUnit.MILLISECONDS) == 0 }

  override def toString = "%d-%b".format(offset, isActive)
}
