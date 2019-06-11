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

import java.util.EnumMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.collection.JavaConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.common.{BrokerEndPointNotAvailableException, Topic, TopicAndPartition}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{MetadataResponse, PartitionState, UpdateMetadataRequest}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
  *
  *  每个分区状态的元数据缓存；该缓存会通过Controller的UpdateMetadataRequest进行更新；
  *  每个broker维护的该元数据缓存是一致的。
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {
  private val stateChangeLogger = KafkaController.stateChangeLogger
  // 记录了每个分区的状态，使用PartitionStateInfo记录Partition状态
  private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]()
  private var controllerId: Option[Int] = None
  // 记录了当前可用的Broker信息，其中使用Broker类记录每个存活Broker的网络位置信息（host、ip、port等）
  private val aliveBrokers = mutable.Map[Int, Broker]()
  // 记录了可用节点的信息
  private val aliveNodes = mutable.Map[Int, collection.Map[SecurityProtocol, Node]]()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = s"[Kafka Metadata Cache on broker $brokerId] "

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
    // 遍历传入的broker
    brokers.foreach { brokerId =>
      // 在aliveNodes中检查是否可以获取到brokerID对应的信息
      val endpoint = getAliveEndpoint(brokerId, protocol) match {
        // 没有找到，根据是否过滤不可用Broker决定如何返回
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        // 找到了，返回对应的Node
        case Some(node) => Some(node)
      }
      // 将可用节点添加到result集合中
      endpoint.foreach(result +=)
    }
    result
  }

  // 获取可用的Node
  private def getAliveEndpoint(brokerId: Int, protocol: SecurityProtocol): Option[Node] =
    aliveNodes.get(brokerId).map { nodeMap =>
      nodeMap.getOrElse(protocol,
        throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not support security protocol `$protocol`"))
    }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    // 获取主题对应的分区字典
    cache.get(topic).map { partitions =>
      // 遍历所有的分区
      partitions.map { case (partitionId, partitionState) =>
        // 根据主题和分区构造TopicAndPartition对象
        val topicPartition = TopicAndPartition(topic, partitionId)
        // 获取分区的Leader副本ID、Leader副本年代信息、ISR信息、Controller年代信息
        val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
        // 获取Leader副本所在的Node，其中记录了host、ip、port
        val maybeLeader = getAliveEndpoint(leaderAndIsr.leader, protocol)
        // 获取分区的AR集合
        val replicas = partitionState.allReplicas
        // 获取分区AR集合中可用的副本
        val replicaInfo = getEndpoints(replicas, protocol, errorUnavailableEndpoints)

        maybeLeader match {
          case None => // 分区的Leader可能宕机了，返回错误码为LEADER_NOT_AVAILABLE
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfo.asJava, java.util.Collections.emptyList())

          case Some(leader) => // Leader副本存活
            // 获取分区的ISR集合
            val isr = leaderAndIsr.isr
            // 获取ISR集合中可用的副本
            val isrInfo = getEndpoints(isr, protocol, errorUnavailableEndpoints)
            // 检测AR集合中的副本是否都是可用的
            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else if (isrInfo.size < isr.size) { // 检测ISR集合中的副本是否都是可用的
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else { // AR和ISR集合的副本都是可用的
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava,
                isrInfo.asJava)
            }
        }
      }
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) { // 加锁
      topics.toSeq.flatMap { topic =>
        // 获取元数据
        getPartitionMetadata(topic, protocol, errorUnavailableEndpoints).map { partitionMetadata =>
          // 将获取的数据构造为TopicMetadata对象
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
        }
      }
    }
  }

  def hasTopicMetadata(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toBuffer
    }
  }

  private def addOrUpdatePartitionInfo(topic: String,
                                       partitionId: Int,
                                       stateInfo: PartitionStateInfo) {
    inWriteLock(partitionMetadataLock) {
      val infos = cache.getOrElseUpdate(topic, mutable.Map())
      infos(partitionId) = stateInfo
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId))
    }
  }

  def getControllerId: Option[Int] = controllerId

  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) {
    inWriteLock(partitionMetadataLock) { // 加锁
      // 获取请求中指定的Controller ID
      controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      // 先清除aliveNodes和aliveBrokers的信息
      aliveNodes.clear()
      aliveBrokers.clear()

      // 遍历请求中的liveBrokers
      updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
        val nodes = new EnumMap[SecurityProtocol, Node](classOf[SecurityProtocol])
        val endPoints = new EnumMap[SecurityProtocol, EndPoint](classOf[SecurityProtocol])
        // 遍历broker得到更新的EndPoint和Node信息
        broker.endPoints.asScala.foreach { case (protocol, ep) =>
          endPoints.put(protocol, EndPoint(ep.host, ep.port, protocol))
          nodes.put(protocol, new Node(broker.id, ep.host, ep.port))
        }
        // 更新aliveBrokers和aliveNodes
        aliveBrokers(broker.id) = Broker(broker.id, endPoints.asScala, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }

      // 根据UpdateMetadataRequest.partitionStates字段更新Cache集合
      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        // 获取Controller ID
        val controllerId = updateMetadataRequest.controllerId
        // 获取Controller年代信息
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          // 删除分区对应的PartitionStateInfo
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(s"Broker $brokerId deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        } else {
          val partitionInfo = partitionStateToPartitionStateInfo(info)
          // 更新PartitionStateInfo
          addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
          stateChangeLogger.trace(s"Broker $brokerId cached leader info $partitionInfo for partition $tp in response to " +
            s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        }
      }
    }
  }

  private def partitionStateToPartitionStateInfo(partitionState: PartitionState): PartitionStateInfo = {
    val leaderAndIsr = LeaderAndIsr(partitionState.leader, partitionState.leaderEpoch, partitionState.isr.asScala.map(_.toInt).toList, partitionState.zkVersion)
    val leaderInfo = LeaderIsrAndControllerEpoch(leaderAndIsr, partitionState.controllerEpoch)
    PartitionStateInfo(leaderInfo, partitionState.replicas.asScala.map(_.toInt))
  }

  def contains(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
    // 从缓存中获取对应的PartitionStateInfo，进行遍历
    cache.get(topic).map { infos =>
      // 从字典中移除对应的PartitionStateInfo
      infos.remove(partitionId)
      // 如果字典为空，可以从cache中移除
      if (infos.isEmpty) cache.remove(topic)
      true
    }.getOrElse(false)
  }

}
