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

import kafka.api.FetchResponsePartitionData
import kafka.api.PartitionFetchInfo
import kafka.common.TopicAndPartition
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}

import scala.collection._

/**
  * @param startOffsetMetadata 记录了在前面读取Log时已经读取到的offset位置
  * @param fetchInfo 记录FetchRequest携带的一些信息，主要是请求的offset以及读取最大字节数
  */
case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionFetchInfo) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
  * The fetch metadata maintained by the delayed fetch operation
  * @param fetchMinBytes 记录需要读取的最小字节数
  * @param fetchOnlyLeader
  * @param fetchOnlyCommitted
  * @param isFromFollower
  * @param fetchPartitionStatus 记录每个分区的FetchPartitionStatus
  */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchOnlyLeader: Boolean,
                         fetchOnlyCommitted: Boolean,
                         isFromFollower: Boolean,
                         fetchPartitionStatus: Map[TopicAndPartition, FetchPartitionStatus]) {

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
                          "onlyLeader:" + fetchOnlyLeader + ", "
                          "onlyCommitted: " + fetchOnlyCommitted + ", "
                          "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
  * @param delayMs 延迟操作的延迟时长
  * @param fetchMetadata FetchMetadata中为FetchRequest中的所有相关分区记录了相关状态，主要用于判断DelayedProduce是否满足执行条件
  * @param replicaManager 此DelayedFetch关联的ReplicaManager对象
  * @param responseCallback 任务满足条件或到期执行时，在DelayedFetch.onComplete()方法中调用的回调函数，其主要功能是创建FetchResponse并添加到RequestChannels中对应的responseQueue队列中
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
    * 主要负责检测是否满足DelayedFetch的执行条件，并在满足条件时调用forceComplete()方法执行延迟操作。
    * 满足下面任一条件，即表示此分区满足DelayedFetch的执行条件：
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
    *         发生Leader副本迁移，当前节点不再是该分区的Leader副本所在的节点。
   * Case B: This broker does not know of some partitions it tries to fetch
    *         当前Broker找不到需要读取数据的分区副本。
   * Case C: The fetch offset locates not on the last segment of the log
    *         开始读取的offset不在activeSegment中，此时可能是发生了Log截断，也有可能是发生了roll操作产生了新的activeSegment。
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
    *         累计读取的字节数超过最小字节数限制。
   *
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0
    // 遍历fetchMetadata中所有Partition的状态
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicAndPartition, fetchStatus) =>
        // 获取前面读取Log时的结束位置
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            // 查找分区的Leader副本，如果找不到就抛出异常
            val replica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
            /**
              * 根据FetchRequest请求的来源设置能读取的最大offset值。
              * 很显然，消费者对应的endOffset是HW，而Follower副本对应的endOffset是LEO
              */
            val endOffset =
              if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark
              else
                replica.logEndOffset

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            /** Case D
              * 检查上次读取后endOffset是否发生变化。
              * 如果没改变，之前读不到足够的数据现在还是读不到，即任务条件依然不满足；
              * 如果变了，则继续下面的检查，看是否真正满足任务执行条件
              */
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, this can happen when the new fetch operation is on a truncated leader
                /** Case C
                  * 此时，endOffset出现减小的情况，跑到baseOffset较小的Segment上了，
                  * 可能是Leader副本的Log出现了truncate操作
                  */
                debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicAndPartition))
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                /** Case C
                  * 此时，fetchOffset虽然依然在endOffset之前，但是产生了新的activeSegment
                  * fetchOffset在较旧的LogSegment，而endOffset在activeSegment
                  */
                debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we need take the partition fetch size as upper bound when accumulating the bytes
                // endOffset和fetchOffset依然在同一个LogSegment中，且endOffset向后移动，那就尝试计算累计的字节数
                accumulatedSize += math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.fetchSize)
              }
            }
          }
        } catch {
          case utpe: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
          case nle: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // Case D
    // Case D 累计读取字节数足够，满足Case 4
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
      // 调用onComplete()方法，在其中会重新读取数据
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    // 重新从Log中读取数据
    val logReadResults = replicaManager.readFromLocalLog(fetchMetadata.fetchOnlyLeader,
      fetchMetadata.fetchOnlyCommitted,
      fetchMetadata.fetchPartitionStatus.mapValues(status => status.fetchInfo))
    // 将读取结果进行封装
    val fetchPartitionData = logReadResults.mapValues(result =>
      FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))
    // 调用回调函数
    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

