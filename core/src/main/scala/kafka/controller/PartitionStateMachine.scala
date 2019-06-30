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
import collection.JavaConversions
import collection.mutable.Buffer
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.utils.{Logging, ReplicationUtils}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 *
  * PartitionStateMachine是Controller Leader用于维护分区状态的状态机
  * @param controller
  */
class PartitionStateMachine(controller: KafkaController) extends Logging {
  // 用于维护KafkaController的上下文信息
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  // Zookeeper客户端，用于与Zookeeper服务器交互
  private val zkUtils = controllerContext.zkUtils
  // 记录了每个分区对应的PartitionState状态
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  // 用于向指定的Broker批量发送请求
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  // 默认的Leader副本选举器，继承自PartitionLeaderSelector
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  // Zookeeper的监听器，用于监听Topic的变化
  private val topicChangeListener = new TopicChangeListener()
  // Zookeeper的监听器，用于监听Topic的删除
  private val deleteTopicsListener = new DeleteTopicsListener()
  // 用于监听分区的修改
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  def startup() {
    // initialize partition state
    initializePartitionState() // 初始化分区的状态
    // set started flag
    hasStarted.set(true) // 标识PartitionStateMachine已启动
    // try to move partitions to online state
    triggerOnlinePartitionStateChange() // 尝试将分区切换到OnlinePartition状态

    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners
  def registerListeners() {
    // 注册TopicChangeListener
    registerTopicChangeListener()
    if(controller.config.deleteTopicEnable) // 判断是否开启了主题删除功能
      // 注册DeleteTopicListener
      registerDeleteTopicListener()
  }

  // de-register topic and partition change listeners
  def deregisterListeners() {
    deregisterTopicChangeListener()
    partitionModificationsListeners.foreach {
      case (topic, listener) =>
        zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
    }
    partitionModificationsListeners.clear()
    if(controller.config.deleteTopicEnable)
      deregisterDeleteTopicListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // clear partition state
    partitionState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
    *
    * 对partitionState集合中的全部分区进行遍历，将OfflinePartition和NewPartition状态的分区转换成OnlinePartition状态
   */
  def triggerOnlinePartitionStateChange() {
    try {
      // 检查ControllerBrokerRequestBatch的leaderAndIsrRequestMap、stopReplicaRequestMap、updateMetadataRequestMap三个集合是否有缓存的请求
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      // 遍历partitionState
      for((topicAndPartition, partitionState) <- partitionState
          if(!controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic))) { // 如果不是标记为删除的Topic
        // 当状态为OfflinePartition或NewPartition，就将其转换为OnlinePartition
        if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build)
      }
      // 发送请求
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      // 检查ControllerBrokerRequestBatch的leaderAndIsrRequestMap、stopReplicaRequestMap、updateMetadataRequestMap三个集合是否有缓存的请求
      brokerRequestBatch.newBatch()
      // 对每个分区都调用handleStateChange()处理状态切换
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }
      // 使用ControllerBrokerRequestBatch对象发送请求
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
    *
    * 管理分区状态的核心方法，控制PartitionState的转换
    *
   * @param topic       The topic of the partition for which the state transition is invoked 主题
   * @param partition   The partition for which the state transition is invoked 分区
   * @param targetState The end state that the partition should be moved to 转换的目标State
   * @param leaderSelector 用于选举Leader副本的PartitionLeaderSelector对象
   * @param callbacks
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)

    // 检测当前PartitionStateMachine对象是否已经启动，只有Controller Leader的PartitionStateMachine对象才启动
    if (!hasStarted.get)
      // 没有启动将抛出StateChangeFailedException异常
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                                            "the partition state machine has not started")
                                              .format(controllerId, controller.epoch, topicAndPartition, targetState))
    // 从partitionState中获取分区的状态，如果没有对应的状态，则初始化为NonExistentPartition
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    try {
      targetState match { // 匹配目标State，检查前置状态是否合法
        case NewPartition => // 将要转换为NewPartition
          // pre: partition did not exist before this
          // 转换为NewPartition状态时，前置状态要为NonExistentPartition，否则会抛出IllegalStateException异常
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          // 将分区状态设置为NewPartition
          partitionState.put(topicAndPartition, NewPartition)

          // 日志记录
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas))
          // post: partition has been assigned replicas
        case OnlinePartition => // 将要转换为OnlinePartition
          // 转换为OnlinePartition状态时，前置状态要为NewPartition、OnlinePartition或OfflinePartition其中一个，否则会抛出IllegalStateException异常
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)

          // 对前置状态进行匹配，根据不同的前置状态做出不同的操作
          partitionState(topicAndPartition) match {
            case NewPartition => // 前置状态为NewPartition，为分区初始化Leader副本和ISR集合
              // initialize leader and isr path for new partition
              initializeLeaderAndIsrForPartition(topicAndPartition)
            case OfflinePartition => // 前置状态为OfflinePartition，为分区选举新的Leader副本
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected 前置状态为OnlinePartition，为分区重新选举新的Leader副本
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }

          // 修改分区状态为OnlinePartition
          partitionState.put(topicAndPartition, OnlinePartition)

          // 日志记录
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // post: partition has a leader
        case OfflinePartition => // 将要转换为OfflinePartition
          // pre: partition should be in New or Online state
          // 转换为OfflinePartition状态时，前置状态要为NewPartition、OnlinePartition或OfflinePartition其中一个，否则会抛出IllegalStateException异常
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive
          // 日志记录
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          // 修改状态为OfflinePartition
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader
        case NonExistentPartition => // 将要转换为NonExistentPartition
          // pre: partition should be in Offline state
          // 转换为NonExistentPartition状态时，前置状态要为OfflinePartition，否则会抛出IllegalStateException异常
          assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
          // 日志记录
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          // 修改状态为NonExistentPartition
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    } catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState() {
    // 遍历集群中的所有分区
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo.get(topicPartition) match { // 获取对应的Leader副本和ISR集合信息
        case Some(currentLeaderIsrAndEpoch) => // 存在Leader副本和ISR集合的信息
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          // 检查分区的Leader所在的Broker是否是可用的
          controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
            // Leader副本所在的Broker可用，初始化为OnlinePartition状态
            case true => // leader is alive
              partitionState.put(topicPartition, OnlinePartition)
            // Leader副本所在的Broker不可用，初始化为OfflinePartition状态
            case false =>
              partitionState.put(topicPartition, OfflinePartition)
          }
        // 没有Leader副本和ISR集合的信息，初始化为NewPartition状态
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState(topicAndPartition))) // 状态不对时会抛出IllegalStateException异常
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
    *
    * NewPartition -> OnlinePartition
    *
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    // 获取分区的AR集合信息
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // 获取AR集合中的可用副本集合
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    // 如果没有可用的副本，则抛出StateChangeFailedException异常，否则尝试选择一个Leader副本
    liveAssignedReplicas.size match {
      case 0 => // 无可用副本
        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        // 抛异常
        throw new StateChangeFailedException(failMsg)
      case _ =>
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader
        // 选择可用副本的第一个副本为Leader
        val leader = liveAssignedReplicas.head
        // 创建LeaderIsrAndControllerEpoch，其中ISR集合是可用的AR集合，leaderEpoch和zkVersion都初始化为0
        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try {
          /**
            * 将LeaderIsrAndControllerEpoch中的消息转换为JSON格式存储到Zookeeper中
            * 路径为：/brokers/topics/[topic_name]/partitions/[partitionId]/state
            */
          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          // 更新ControllerContext的partitionLeadershipInfo中的记录
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          // 添加LeaderAndISRRequest，等待发送
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
        } catch {
          case e: ZkNodeExistsException => // Zookeeper结点已存在异常
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
    *
    * OfflinePartition, OnlinePartition -> OnlinePartition
    *
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
   */
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    // handle leader election for the partitions whose leader is no longer alive
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      // 记录Zookeeper中的路径是否更新成功，初始标记为false
      var zookeeperPathUpdateSucceeded: Boolean = false
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
      while(!zookeeperPathUpdateSucceeded) { // 当Zookeeper中的路径没有成功更新时
        // 从Zookeeper中获取分区当前的Leader副本、ISR集合、zkVersion等信息，如果不存在将抛出StateChangeFailedException异常
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
        // 当前的Leader及ISR信息
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
        // 当前的Controller年代信息
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
        // 校验年代信息
        if (controllerEpoch > controller.epoch) {
          val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                         "already written by another controller. This probably means that the current controller %d went through " +
                         "a soft failure and another controller was elected with epoch %d.")
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg)
        }
        // elect new leader or throw exception
        /**
          * 使用指定的PartitionLeaderSelector以及当前的LeaderAndIsr信息，选举新的Leader副本和ISR集合
          * 返回的第二个值为需要接收LeaderAndIsrRequest的副本集合
          */
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        /**
          * 将新的LeaderAndIsr信息转换为JSON格式保存到Zookeeper中，
          * 路径为：/brokers/topics/[topic_name]/partitions/[partitionId]/state
          * 第一个返回值表示是否更新成功，第二个返回值表示更新后的zkVersion
          */
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)

        // 更新记录值
        newLeaderAndIsr = leaderAndIsr
        newLeaderAndIsr.zkVersion = newVersion
        zookeeperPathUpdateSucceeded = updateSucceeded // 更新成功后修改为true时，跳出循环
        // 记录分配后该分区的副本集合
        replicasForThisPartition = replicas
      }

      // 维护ControllerContext的partitionLeadershipInfo内的记录
      val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))

      // 获取分区新的AR集合
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      // 添加LeaderAndISRRequest，等待发送
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case lenne: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce)
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  // 注册TopicChangeListener监听器，监听路径为/brokers/topics
  private def registerTopicChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  def registerPartitionChangeListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(topic))
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  // 移除主题分区上的监听器
  def deregisterPartitionChangeListener(topic: String) = {
    // 从Zookeeper中移除监听分区修改的监听器
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    // 从partitionModificationsListeners移除记录的主题的监听器
    partitionModificationsListeners.remove(topic)
  }

  // 注册DeleteTopicListener监听器，监听路径为/admin/delete_topics
  private def registerDeleteTopicListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def deregisterDeleteTopicListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    // 从Zookeeper中获取分区信息
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      // 不存在时抛出StateChangeFailedException异常
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg)
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
    * TopicChangeListener负责管理Topic的增删，它监听/brokers/topics节点的子节点的变化
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            val currentChildren = { // 获取/brokers/topics下的子节点集合
              import JavaConversions._
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              (children: Buffer[String]).toSet
            }
            // 过滤，得到新添加的Topic
            val newTopics = currentChildren -- controllerContext.allTopics
            // 过滤，得到删除的Topic
            val deletedTopics = controllerContext.allTopics -- currentChildren
            // 更新ControllerContext的allTopics集合
            controllerContext.allTopics = currentChildren

            // 从Zookeeper的/brokers/topics/[topic_name]路径加载新增Topic的分区信息和AR集合信息
            val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
            // 更新ControllerContext的partitionReplicaAssignment记录的AR集合
            // 去掉删除主题的副本，添加新增主题的副本
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            // 调用KafkaController的onNewTopicCreation()处理新增Topic
            if(newTopics.size > 0)
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
        }
      }
    }
  }

  /**
   * Delete topics includes the following operations -
   * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
   * 2. If there are topics to be deleted, it signals the delete topic thread
    *
    * 主题删除操作监听器
    * 1. 当被删除的主题存在时，将其添加到记录删除主题的缓存中；
    * 2. 当由主题被删除时，会通知DeleteTopicThread线程
   */
  class DeleteTopicsListener() extends IZkChildListener with Logging {
    this.logIdent = "[DeleteTopicsListener on " + controller.config.brokerId + "]: "
    val zkUtils = controllerContext.zkUtils

    /**
     * Invoked when a topic is being deleted
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        // 获取待删除的主题的集合
        var topicsToBeDeleted = {
          import JavaConversions._
          (children: Buffer[String]).toSet
        }
        debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
        // 过滤得到不存在的删除主题
        val nonExistentTopics = topicsToBeDeleted.filter(t => !controllerContext.allTopics.contains(t))
        if(nonExistentTopics.size > 0) {
          warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
          // 对于不存在的主题，直接将其在/admin/delete_topics下对应的节点删除
          nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
        }
        // 过滤掉不存在的待删除主题
        topicsToBeDeleted --= nonExistentTopics
        if(topicsToBeDeleted.size > 0) {
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic => // 遍历所有待删除的主题，检查待删除主题是否处于不可删除的情况
            // 第1项：检查待删除主题中是否有分区正在进行"优先副本"选举
            val preferredReplicaElectionInProgress =
              controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
            // 第2项：检查待删除主题中是否有分区正在进行副本重新分配
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if(preferredReplicaElectionInProgress || partitionReassignmentInProgress)
              // 有1项满足，就将待删除主题标记为不可删除
              controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
          }
          // add topic to deletion list
          // 通过该方法将待删除的主题放入到topicsToBeDeleted集合，将待删除的主题的分区放入到partitionsToBeDeleted集合
          controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      }
    }

    /**
     *
     * @throws Exception
   *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
    }
  }

  /**
    * 监听/brokers/topics/[topic_name]节点中的数据变化，主要用于监听一个Topic的分区变化。
    * 并不对分区的删除进行处理，Topic的分区数量是不能减少的。
    */
  class PartitionModificationsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object) {
      inLock(controllerContext.controllerLock) {
        try {
          info(s"Partition modification triggered $data for path $dataPath")
          // 从Zookeeper中获取Topic的Partition记录
          val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
          // 过滤出新增分区的记录
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
          // 主题正在进行删除操作，输出日志后直接返回
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          else {
            if (partitionsToBeAdded.size > 0) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              // 将新增分区信息添加到ControllerContext中
              controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
              // 切换新增分区及其副本的状态，最终使其上线对外服务
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet.toSet)
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String) {
      // this is not implemented for partition change
    }
  }
}

/**
  * 分区的状态是通过PartitionState接口定义；
  * sealed关键字可以修饰类和特质（特质）。密封类提供了一种约束：不能在类定义的文件之外定义任何新的子类。
  */
sealed trait PartitionState { def state: Byte }
// 分区被创建后就处于此状态。此时分区可能已经被分配了AR集合，但是还没有指定Leader副本和ISR集合
case object NewPartition extends PartitionState { val state: Byte = 0 }
// 分区成功选举出Leader副本之后，分区会转换为此状态
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
// 已经成功选举出分区的Leader副本后，但Leader副本发生宕机，则分区转换为此状态。或者，新创建的分区直接转换为此状态
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
// 分区从来没有被创建或是分区被创建之后被又删除掉了，这两种场景下的分区都处于此状态
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }
