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


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

/**
  * @param requiredOffset requiredOffset是ProducerRequest中追加到此分区的最后一个消息的offset
  * @param responseStatus responseStatus字段主要用来记录ProducerResponse中的错误码
  */
case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  /**
    * 表示是否在等待ISR集合中其他副本与Leader副本同步requiredOffset之前的消息
    * 如果ISR集合中所有副本已经完成了requiredOffset之前消息的同步，此值被设置为false
    */
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.errorCode, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
  * @param produceRequiredAcks 记录了ProduceRequest中acks字段的值
  * @param produceStatus 记录了每个Partition的ProducePartitionStatus
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
  * @param delayMs DelayedProduce的延迟时长
  * @param produceMetadata ProduceMetadata对象。ProduceMetadata中为一个ProducerRequest中的所有相关分区记录了一些追加消息后的返回结果，主要用于判断DelayedProduce是否满足执行条件
  * @param replicaManager 此DelayedProduce关联的ReplicaManager对象
  * @param responseCallback 任务满足条件或到期执行时，在DelayedProduce.onComplete()方法中调用的回调函数
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  /**
    * 对produceMetadata字段中的produceStatus集合进行设置
    * 根据前面写入消息返回的结果，设置ProducePartitionStatus的acksPending字段和responseStatus字段的值
    */
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    // 对应分区的写入操作成功，则等待ISR集合中的副本完成同步；如果写入操作异常，则该分区不需要等待
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      // 预设错误码，如果ISR集合中副本在此请求超时之前顺利完成了同步，会清楚该错误码
      status.responseStatus.errorCode = Errors.REQUEST_TIMED_OUT.code
    } else {
      // 如果追加日志已经抛出异常，则不必等待此Partition对应的ISR返回ACK了
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
    *         当前broker不再是Leader副本了（可能发生Leader迁移），则需要在响应中记录错误
   * Case B: This broker is the leader:
    *         当前broker还是Leader副本
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
    *         如果在In-Sync副本完成了同步，但在这个过程中发生了本地错误，则需要在响应中记录错误
   *   B.2 - Otherwise, set the response with no error.
    *         其他情况，清除响应中的错误
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    // 遍历produceMetadata中的所有分区的状态
    produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
      trace("Checking produce satisfaction for %s, current status %s"
        .format(topicAndPartition, status))
      // skip those partitions that have already been satisfied
      if (status.acksPending) { // 检查此分区是否已经满足DelayedProduce执行条件
        // 获取对应的Partition对象
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            /**
              * 检查此分区的HW位置是否大于requiredOffset
              * 这里涉及Partition类中的checkEnoughReplicasReachOffset(...)方法
              */
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            // 条件A：找不到此分区的Leader，记录错误
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
        if (errorCode != Errors.NONE.code) { // 条件B.1： 出现异常
          // Case B.1
          status.acksPending = false
          // 记录错误
          status.responseStatus.errorCode = errorCode
        } else if (hasEnough) { // 条件B.2：此分区Leader副本的HW大于对应的requiredOffset
          // Case B.2
          status.acksPending = false
          // 清除错误
          status.responseStatus.errorCode = Errors.NONE.code
        }
      }
    }

    // check if each partition has satisfied at lease one of case A and case B
    // 检查全部的分区是否都已经符合DelayedProduce的执行条件
    if (! produceMetadata.produceStatus.values.exists(p => p.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete() {
    // 根据ProduceMetadata记录的相关信息，为每个Partition产生响应状态
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    // 调用responseCallback回调函数
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

