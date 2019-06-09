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

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.{LogReadResult, LogOffsetMetadata}
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

/**
  * @param brokerId 标识该副本所在的Broker的id
  * @param partition 此副本对应的分区
  * @param time
  * @param initialHighWatermarkValue
  * @param log 本地副本对应的Log对象，远程副本的该字段为空，可以通过这个字段区分本地副本和远程副本
  */
class Replica(val brokerId: Int,
              val partition: Partition,
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  // 用于记录HighWatermark值，由Leader负责更新和维护
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  /**
    * 对于本地副本，此字段记录的是追加到Log中的最新消息的offset，可以直接从Log.nextOffsetMetadata字段中获取；
    * 对于远程副本，此字段含义相同，但是由其他Broker发送请求来更新此值，并不能直接从本地获取到
    */
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic = partition.topic
  val partitionId = partition.partitionId

  // 是否是本地副本，根据log字段是否为null来判断
  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }

  // 记录Follower副本最后一次追赶上Leader的时间戳
  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds)

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  def updateLogReadResult(logReadResult : LogReadResult) {
    // 更新LEO
    logEndOffset = logReadResult.info.fetchOffsetMetadata

    /* If the request read up to the log end offset snapshot when the read was initiated,
     * set the lastCaughtUpTimeMsUnderlying to the current time.
     * This means that the replica is fully caught up.
     * 如果此时Follower副本的同步进度完全追赶上Leader副本，则更新lastCaughtUpTimeMsUnderlying为当前时间
     */
    if(logReadResult.isReadFromLogEnd) {
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  // _=即logEndOffset的setter方法，该方法表示更新LEO
  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      // 本地副本不能直接更新LEO，由Log.logEndOffsetMetadata字段决定
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {
      // 远程副本LEO是通过请求进行更新的
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  // 获取LEO
  def logEndOffset =
    if (isLocal)
      // 本地副本由Log.logEndOffsetMetadata字段获取
      log.get.logEndOffsetMetadata
    else
      // 远程副本通过logEndOffsetMetadata获取
      logEndOffsetMetadata

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      // 只有本地副本可以更新HW
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      // 如果是本地副本，由Log对象负责更新
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      // 否则抛出异常
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
