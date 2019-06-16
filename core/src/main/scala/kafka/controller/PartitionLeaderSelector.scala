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
package kafka.controller

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.utils.Logging
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.server.{ConfigType, KafkaConfig}

// 分区Leader选择器
trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected
    *                                    需要进行Leader副本选举的分区
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
    *                                    当前Leader副本信息、ISR信息
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest. 选举后的新Leader副本和新ISR集合信息，以及需要接收LeaderAndIsrRequest的BrokerID
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
  *
  * 1. 如果在ISR集合中存在至少一个可用的副本，则从ISR集合中选择新的Leader副本，当前ISR集合为新ISR集合。
  * 2. 如果ISR集合中没有可用的副本且“Unclean leader election”配置被禁用，那么就抛出异常。
  * 3. 如果“Unclean leader election”被开启，则从AR集合中选择新的Leader副本和ISR集合。
  * 4. 如果AR集合中没有可用的副本，抛出异常。
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {

    // 获取当前分区的AR集合
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) => // 存在AR集合
        // 过滤得到AR集合中可用的副本
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        // 过滤得到ISR集合中可用的副本
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
        // 当前Controller年代信息及Leader副本的zkVersion
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

        // 尝试从ISR集合的可用副本中选取一个Leader副本
        val newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
          case true => // ISR集合中没有可用副本，且没有开启unclean leader election，抛出NoReplicaOnlineException异常
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
            }

            debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
              .format(topicAndPartition, liveAssignedReplicas.mkString(",")))

            // 走到这里说明ISR中没有可用副本，但开启了unclean leader election
            // 尝试从AR集合的可用副本中选取一个Leader副本
            liveAssignedReplicas.isEmpty match {
              case true => // AR集合中也没有可用副本，抛出NoReplicaOnlineException异常
                throw new NoReplicaOnlineException(("No replica for partition " +
                  "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
              case false => // AR集合中有可用的副本
                ControllerStats.uncleanLeaderElectionRate.mark()
                // 选择AR集合可用副本的第一个作为Leader副本
                val newLeader = liveAssignedReplicas.head
                warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                     .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                // 构造LeaderAndIsr对象，有新Leader副本、Leader年代信息 + 1、ISR集合只有新Leader副本，当前Leader的zkVersion + 1
                new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }
          case false => // ISR集合中有可用的副本
            // 从AR集合中过滤得到还存活于ISR集合中的副本集，该副本集将作为新的ISR集合，并选择其中的第一个副本作为Leader副本
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            val newLeader = liveReplicasInIsr.head
            debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
                  .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
            // 构造LeaderAndIsr对象，有新Leader副本、Leader年代信息 + 1、新的ISR集合，当前Leader的zkVersion + 1
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
        }
        info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
        // 返回新的Leader副本信息及新的AR集合，并需要向AR集合所有存活的副本发送LeaderAndIsrRequest
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        // AR集合为空，抛出NoReplicaOnlineException异常
        throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
    }
  }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
  *
  * 选取的新Leader副本必须在新指定的AR集合中且同时在当前ISR集合中，
  * 当前ISR集合为新ISR集合，接收LeaderAndIsrRequest的副本是新指定的AR集合中的副本。
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // 获取指定分区重新分配到的ISR集合
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    // 当前Leader年代信息和zkVersion
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    // 过滤得到存活的且存在于当前ISR集合中的副本，取第1个作为新的Leader副本
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    // 取第1个
    val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
    newLeaderOpt match {
      // 存在，将其作为新的Leader副本，当前ISR集合为新的ISR集合，向新分配到的AR集合中的副本发送LeaderAndIsrRequest请求
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      // 不存在，抛出NoReplicaOnlineException异常
      case None =>
        reassignedInSyncReplicas.size match {
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
              " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
              "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
        }
    }
  }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
  *
  * 如果“优先副本”（即AR的第1个副本）可用且在ISR集合中且存活，则选取其为Leader副本，当前的ISR集合为新的ISR集合，
  * 并向AR集合中所有可用副本发送LeaderAndIsrRequest，
  * 否则会抛出异常。
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // 分区的AR集合
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // AR集合中第1个副本为"优先副本"
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader
    // 检查该"优先副本"是不是当前的Leader副本
    // 获取当前的Leader副本
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      // 如果"优先副本"就是当前的Leader副本，抛出LeaderElectionNotNeededException异常
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      // "优先副本"不是当前的Leader副本
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Trigerring preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      // "优先副本"可用且存在于ISR集合中
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        // 将"优先副本"作为Leader副本，当前ISR集合为新的ISR集合，并向AR集合所有的副本发送LeaderAndIsrRequest请求
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
          "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
      }
    }
  }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
  *
  * 从当前ISR集合中排除正在关闭的副本后作为新的ISR集合，从新ISR集合中选择新的Leader，
  * 需要向AR集合中可用的副本发送LeaderAndIsrRequest
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // 当前Leader副本的年代信息和zkVersion
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    // 当前的Leader副本
    val currentLeader = currentLeaderAndIsr.leader

    // 当前AR集合
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)

    // 当前可用的Broker的ID
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds

    // 过滤得到AR集合中可用的副本
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))

    // 过滤得到ISR集合中可用的副本
    val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))

    // 从AR集合中过滤得到存在于ISR集合中可用的副本，选择第1个作为Leader副本
    liveAssignedReplicas.filter(newIsr.contains).headOption match {
      case Some(newLeader) => // 第1个存在，构建LeaderAndIsr，过滤得到的ISR集合为新的ISR集合，并向AR集合所有可用的副本发送LeaderAndIsrRequest请求
        debug("Partition %s : current leader = %d, new leader = %d".format(topicAndPartition, currentLeader, newLeader))
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
      case None =>
        // 第1个不存在，抛出StateChangeFailedException异常
        throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
          " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
  * 没有进行Leader选举，而是将currentLeaderAndIsr和分区的AR集合直接返回
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  this.logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    // 直接返回了currentLeaderAndIsr，分区的AR集合
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
