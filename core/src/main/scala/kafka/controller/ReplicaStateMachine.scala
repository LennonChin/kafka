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

import collection._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
  *
  * ReplicaStateMachine是Controller Leader用于维护副本状态的状态机。
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  // 记录每个副本对应的ReplicaState状态
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  // 用于监听Broker的变化，例如Broker宕机或重新上线等事件
  private val brokerChangeListener = new BrokerChangeListener()
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)

  // 标识状态机是否启动
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup() {
    // initialize replica state
    initializeReplicaState() // 初始化replicaState集合
    // set started flag
    hasStarted.set(true) // 标识已启动
    // move all Online replicas to Online
    // 尝试将所有可用副本转换为OnlineReplica状态
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine
  def registerListeners() {
    // register broker change listener
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine
  def deregisterListeners() {
    // de-register broker change listener
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // reset replica state
    replicaState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.size > 0) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
    *
    * 该方法用于控制ReplicaState转换
    *
   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {

    // 获取主题、分区和副本ID
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica

    val topicAndPartition = TopicAndPartition(topic, partition)

    // 如果状态机没有启动则抛出异常
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))

    // 获取副本当前的状态，如果没有对应的状态就设置初始值为NonExistentReplica
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {

      // 获取分区的AR集合
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)

      // 针对目标状态进行匹配
      targetState match {
        case NewReplica => // 目标状态是NewReplica
          // 检查前置状态是否合理，必须是NonExistentReplica
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          // 获取分区的Leader副本、ISR集合、Controller年代等信息
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            // 能够获取到Leader副本、ISR等信息
            case Some(leaderIsrAndControllerEpoch) =>
              // 当前副本是Leader副本，Leader副本不可以处于NewReplica状态
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")

              // 向对应副本发送LeaderAndIsrRequest请求，并发送UpdateMetadataRequest给所有可用的Broker
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            // 未能获取Leader、ISR等信息
            case None => // new leader request will be sent to this replica when one gets elected
          }

          // 更新状态
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                            targetState))
        case ReplicaDeletionStarted => // 目标状态是ReplicaDeletionStarted
          // 检查前置状态是否合理，必须是OfflineReplica
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          // 更新状态
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          // 向副本发送StopReplicaRequest请求
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionIneligible => // 目标状态是ReplicaDeletionIneligible
          // 检查前置状态是否合理，必须是ReplicaDeletionStarted
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          // 更新状态
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionSuccessful => // 目标状态是ReplicaDeletionSuccessful
          // 检查前置状态是否合理，必须是ReplicaDeletionStarted
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          // 更新状态
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case NonExistentReplica => // 目标状态是NonExistentReplica
          // 检查前置状态是否合理，必须是ReplicaDeletionSuccessful
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition
          // 获取分区对应的AR集合
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          // 从AR集合中删除该副本
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          // 删除副本状态
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case OnlineReplica => // 目标状态是OnlineReplica
          // 检查前置状态是否合理，必须是NewReplica、OnlineReplica、OfflineReplica、ReplicaDeletionIneligible
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          replicaState(partitionAndReplica) match {
            case NewReplica => // 当前状态是NewReplica
              // add this replica to the assigned replicas list for its partition
              // 获取分区的AR集合
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)

              // 分区当前的AR集合中不包含指定的副本时，将该副本添加到AR集合中
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                                targetState))
            case _ => // 当前状态是除NewReplica以外的其他状态（OnlineReplica、OfflineReplica、ReplicaDeletionIneligible）
              // check if the leader for this partition ever existed
              // 获取分区的Leader副本信息
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                // 如果存在Leader副本
                case Some(leaderIsrAndControllerEpoch) =>
                  // 向副本发送LeaderAndIsrRequest请求，并向集群中所有可用的Broker发送UpdateMetadataRequest请求
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  // 更改状态为OnlineReplica
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica => // 目标状态是OfflineReplica
          // 检查前置状态是否合理，必须是NewReplica、OnlineReplica、OfflineReplica、ReplicaDeletionIneligible
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader
          // 向该副本发送StopReplicaRequest请求，但不会删除副本（deletePartition为false）
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            // 获取分区的Leader副本
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              // 能够获取到Leader副本
              case Some(currLeaderIsrAndControllerEpoch) =>
                // 从ISR集合中移除该副本
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  // ISR集合移除该副本后Leader副本还存在，即移除的副本不是Leader副本
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    // 当前的ISR集合
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) { // 当前分区并不是将要删除的分区
                      // 向其他可用副本发送LeaderAndIsrRequest请求，并向集群中所有可用的Broker发送UpdateMetadataRequest
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    // 更新状态
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                    false // 能找到Leader副本，返回false
                  case None =>
                    true // 找不到Leader副本，返回true
                }
              case None =>
                true // 找不到Leader副本，返回true
            }
          if (leaderAndIsrIsEmpty && !controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition))
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    // 获取主题的副本集合
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    // 获取所有副本的状态
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    // 判断这些副本的状态是否都是ReplicaDeletionSuccessful
    replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
    *
    * 根据ControllerContext的partitionLeadershipInfo中记录的Broker状态来设置每个副本的初始状态
   */
  private def initializeReplicaState() {
    // 遍历ControllerContext中记录的所有分区及对应的AR副本集
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition

      // 遍历每个分区的AR副本集
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)

        // 遍历可用Broker的ID集
        controllerContext.liveBrokerIds.contains(replicaId) match {
          // 如果副本是可用的，则设置为OnlineReplica状态
          case true => replicaState.put(partitionAndReplica, OnlineReplica)
          case false => // 否则设置为ReplicaDeletionIneligible状态
            // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
            // This is required during controller failover since during controller failover a broker can go down,
            // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
            replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
    * 唯一的Zookeeper Listener，监听/brokers/ids节点下的子节点变化，主要负责处理Broker的上线和故障下线。
    * 当Broker上线时会在/brokers/ids下创建临时节点，下线时会删除对应的临时节点。
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) { // 需要在状态机启动的情况下执行
          ControllerStats.leaderElectionTimer.time { // 计时操作
            try {
              // 获取Zookeeper中的Broker列表、得到BrokerID集合
              val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
              val curBrokerIds = curBrokers.map(_.id)
              // ControllerContext中记录的当前可用Broker的ID集合
              val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
              // 过滤得到新增的Broker列表
              val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
              // 过滤得到出现故障的Broker的ID集合
              val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
              val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
              // 更新ControllerContext中记录的可用Broker列表
              controllerContext.liveBrokers = curBrokers

              // 对新增Broker的列表、出现故障Broker的列表以及Zookeeper中的Broker列表进行排序
              val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
              val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
              val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))

              // 创建Controller和新增Broker的网络连接
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
              // 关闭Controller和出现故障的Broker的网络连接
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)

              // 调用onBrokerStartup()方法和onBrokerFailure()方法分别处理新增Broker和下线Broker
              if(newBrokerIds.size > 0)
                controller.onBrokerStartup(newBrokerIdsSorted)
              if(deadBrokerIds.size > 0)
                controller.onBrokerFailure(deadBrokerIdsSorted)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

// 副本状态
sealed trait ReplicaState { def state: Byte }
// 创建新Topic或进行副本重新分配时，新创建的副本就处于这个状态。处于此状态的副本只能成为Follower副本。
case object NewReplica extends ReplicaState { val state: Byte = 1 }
// 副本开始正常工作时处于此状态，处在此状态的副本可以成为Leader副本，也可以成为Follower副本。
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
// 副本所在的Broker下线后，会转换为此状态。
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
// 刚开始删除副本时，会先将副本转换为此状态，然后开始删除操作。
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
// 副本被成功删除后，副本状态会处于此状态。
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
// 如果副本删除操作失败，会将副本转换为此状态。
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}
// 副本被成功删除后最终转换为此状态。
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }
