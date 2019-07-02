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


import kafka.server.ConfigType
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{StopReplicaResponse, AbstractRequestResponse}

import collection.mutable
import collection.JavaConverters._
import kafka.utils.{ShutdownableThread, Logging}
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._
import collection.Set
import kafka.common.TopicAndPartition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 *    is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 *    be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 *    for deletion in the following scenarios -
 *    3.1 broker hosting one of the replicas for that topic goes down
 *    3.2 partition reassignment for partitions of that topic is in progress
 *    3.3 preferred replica election for partitions of that topic is in progress
 *    (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 preferred replica election for partitions of that topic completes
 *    4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 * @param controller
 * @param initialTopicsToBeDeleted The topics that are queued up for deletion in zookeeper at the time of controller failover
 * @param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,
                           initialTopicsIneligibleForDeletion: Set[String] = Set.empty) extends Logging {
  this.logIdent = "[Topic Deletion Manager " + controller.config.brokerId + "], "
  val controllerContext = controller.controllerContext
  // 分区状态机
  val partitionStateMachine = controller.partitionStateMachine
  // 副本状态机
  val replicaStateMachine = controller.replicaStateMachine
  // 记录将要被删除的主题集合
  val topicsToBeDeleted: mutable.Set[String] = mutable.Set.empty[String] ++ initialTopicsToBeDeleted
  // 记录将要被删除的分区集合
  val partitionsToBeDeleted: mutable.Set[TopicAndPartition] = topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)
  val deleteLock = new ReentrantLock()
  // 记录不可被删除的主题
  val topicsIneligibleForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++
    (initialTopicsIneligibleForDeletion & initialTopicsToBeDeleted)
  // 用于其他线程与deleteTopicsThread删除线程同步
  val deleteTopicsCond = deleteLock.newCondition()
  // 用于标识Topic删除操作是否开始
  val deleteTopicStateChanged: AtomicBoolean = new AtomicBoolean(false)
  // 用于删除Topic的后台线程
  var deleteTopicsThread: DeleteTopicsThread = null

  // 从配置中读取是否开启了Topic删除功能
  val isDeleteTopicEnabled = controller.config.deleteTopicEnable // delete.topic.enable

  /**
   * Invoked at the end of new controller initiation
    * 启动时会调用该方法进行初始化
   */
  def start() {
    if (isDeleteTopicEnabled) { // 判断是否开启了Topic删除功能
      // 创建deleteTopicsThread删除线程
      deleteTopicsThread = new DeleteTopicsThread()
      if (topicsToBeDeleted.size > 0)
        // 当存在将要被删除的Topic集合时，将deleteTopicStateChanged状态置为true
        deleteTopicStateChanged.set(true)
      // 启动deleteTopicsThread删除线程
      deleteTopicsThread.start()
    }
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
   */
  def shutdown() {
    // Only allow one shutdown to go through
    if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
      // Resume the topic deletion so it doesn't block on the condition
      resumeTopicDeletionThread()
      // Await delete topic thread to exit
      deleteTopicsThread.awaitShutdown()
      topicsToBeDeleted.clear()
      partitionsToBeDeleted.clear()
      topicsIneligibleForDeletion.clear()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) { // 检测是否开启了主题删除功能
      topicsToBeDeleted ++= topics // 待删除主题放入到topicsToBeDeleted
      partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic) // 待删除主题的分区放入到partitionsToBeDeleted
      resumeTopicDeletionThread() // 唤醒DeleteTopicThread线程
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished
   *    preferred replica election
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    if(isDeleteTopicEnabled) { // 检查是否开启主题删除功能
      // 找出给定Topic集合与删除Topic集合的交集，将该交集中的Topic从topicsIneligibleForDeletion集合中删除
      val topicsToResumeDeletion = topics & topicsToBeDeleted
      if(topicsToResumeDeletion.size > 0) {
        topicsIneligibleForDeletion --= topicsToResumeDeletion
        // 唤醒DeleteTopicsThread线程处理待删除的Topic
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
   * if it has received a response for all replicas of a topic to be deleted
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    if(isDeleteTopicEnabled) { // 检查是否开启了主题删除
      // 过滤出正在等待删除的主题的副本
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if(replicasThatFailedToDelete.size > 0) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug("Deletion failed for replicas %s. Halting deletion for topics %s"
          .format(replicasThatFailedToDelete.mkString(","), topics))
        // 将这些副本的状态转换为ReplicaDeletionIneligible
        controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible)
        // 标记这些副本所属的Topic不可用
        markTopicIneligibleForDeletion(topics)
        // 唤醒DeleteTopicsThread线程
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * 3. preferred replica election in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) { // 判断是否开启了主题删除功能
      val newTopicsToHaltDeletion = topicsToBeDeleted & topics
      // 将传入的主题添加到topicsIneligibleForDeletion集合中，即表示他们不可被删除
      topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if(newTopicsToHaltDeletion.size > 0)
        info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
    }
  }

  def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) { // 开启了主题删除功能
      // topicsIneligibleForDeletion中包含该主题
      topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  // 主题是否正处于正在删除的过程中
  def isTopicDeletionInProgress(topic: String): Boolean = {
    if(isDeleteTopicEnabled) { // 开启了主题删除功能
      // 是否至少有一个副本处于ReplicaDeletionStarted状态
      controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
    } else
      false
  }

  def isPartitionToBeDeleted(topicAndPartition: TopicAndPartition) = {
    if(isDeleteTopicEnabled) {
      partitionsToBeDeleted.contains(topicAndPartition)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted.contains(topic)
    } else
      false
  }

  /**
   * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
   * controllerLock should be acquired before invoking this API
    *
    * 等待直到删除主题的操作被触发：
    * 1. deleteTopicsThread线程正在运行；
    * 2. deleteTopicStateChanged状态为true，且CAS修改为false成功
   */
  private def awaitTopicDeletionNotification() {
    inLock(deleteLock) {
      // deleteTopicsThread线程在运行，但无法将deleteTopicStateChanged切换为false，将等待
      while(deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {
        debug("Waiting for signal to start or continue topic deletion")
        deleteTopicsCond.await()
      }
    }
  }

  /**
   * Signals the delete-topic-thread to process topic deletion
   */
  private def resumeTopicDeletionThread() {
    deleteTopicStateChanged.set(true) // 切换状态
    inLock(deleteLock) {
      // 唤醒主题删除线程
      deleteTopicsCond.signal()
    }
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
   * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    // 过滤得到成功删除的副本
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
    // 将成功删除的副本的状态转换为ReplicaDeletionSuccessful
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
    // 唤醒DeleteTopicsThread线程
    resumeTopicDeletionThread()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    /**
      * 1. 主题是待删除主题，即主题没有完全完成删除操作；
      * 2. 主题并不处于正在删除的过程中，即主题的删除操作还没开始；
      * 3. 主题并不处于topicsIneligibleForDeletion集合中，即主题没有标记为不可删除。
      */
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic))
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   *@param topic Topic for which deletion should be retried
   */
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    // 过滤得到失败的副本集
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
    info("Retrying delete topic for topic %s since replicas %s were not successfully deleted"
      .format(topic, failedReplicas.mkString(",")))
    // 将失败的副本集都重置为OfflineReplica状态
    controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String) {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    // 移除即将被删除的主题上的PartitionModificationsListener监听器
    partitionStateMachine.deregisterPartitionChangeListener(topic)
    // 从副本状态机中获取所有状态为ReplicaDeletionSuccessful的副本集
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    // 使用副本状态机将这些状态为ReplicaDeletionSuccessful的副本集的状态转换为NonExistentReplica
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
    // 获取即将被删除主题的所有分区
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // move respective partition to OfflinePartition and NonExistentPartition state
    // 将即将被删除主题的所有分区的状态转换为OfflinePartition
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
    // 将即将被删除主题的所有分区的状态转换为NonExistentPartition
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
    // 从topicsToBeDeleted中移除已经处理的主题
    topicsToBeDeleted -= topic
    // 从partitionsToBeDeleted中移除已经处理的主题的分区
    partitionsToBeDeleted.retain(_.topic != topic)
    // 维护Zookeeper中的信息
    val zkUtils = controllerContext.zkUtils
    zkUtils.zkClient.deleteRecursive(getTopicPath(topic))
    zkUtils.zkClient.deleteRecursive(getEntityConfigPath(ConfigType.Topic, topic))
    zkUtils.zkClient.delete(getDeleteTopicPath(topic))
    controllerContext.removeTopic(topic) // 情况ControllerContext中的缓存
  }

  /**
   * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
   * It invokes the delete partition callback for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  private def onTopicDeletion(topics: Set[String]) {
    info("Topic deletion callback for %s".format(topics.mkString(",")))
    // send update metadata so that brokers stop serving data for topics to be deleted
    // 获取指定主题的所有分区
    val partitions = topics.flatMap(controllerContext.partitionsForTopic)
    // 向可用Broker发送UpdateMetadataRequest请求
    controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
    // 按照Topic进行分组
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    // 开始分区的删除操作
    topics.foreach { topic =>
      // 删除分区方法
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).map(_._1).toSet)
    }
  }

  /**
   * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
   * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
   * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
   * is never retried. A topic is removed from the in progress list when
   * 1. Either the topic is successfully deleted OR
   * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
   * the replicas a StopReplicaRequest (delete=true)
   * This callback does the following things -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
   *@param replicasForTopicsToBeDeleted
   */
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    replicasForTopicsToBeDeleted.groupBy(_.topic) // 根据主题进行分区
      .foreach { case(topic, replicas) => // 遍历进行删除
      // 获取Topic中所有可用的副本
      var aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic.equals(topic))
      // 获取Topic中所有不可用的副本
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      // 获取Topic中所有已经完成删除的副本
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      // 第一次删除（或重试）的副本集合
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      // 将不可用副本转换为ReplicaDeletionIneligible状态
      replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      /**
        * 将待删除的副本转换为OfflineReplica状态，
        * 此步骤会发送StopReplicaRequest到待删除的副本（不会删除副本），
        * 同时还会向可用的Broker发送LeaderAndIsrRequest和UpdateMetadataRequest，将副本从ISR集合中删除
        */
      replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
      debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
      // 将待删除的副本转换为ReplicaDeletionStarted状态，此步骤会向可用副本发送StopReplicaRequest（删除副本）
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
        // 注意这里使用到了deleteTopicStopReplicaCallback回调函数
        new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
      if(deadReplicasForTopic.size > 0) {
        debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
        // 标记当前Topic不可删除，即添加到topicsIneligibleForDeletion集合汇总
        markTopicIneligibleForDeletion(Set(topic))
      }
    }
  }

  /**
   * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
   *    deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
    info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
    // 获取需要删除的每个分区的副本
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    // 调用startReplicaDeletion()方法进行删除
    startReplicaDeletion(replicasPerPartition)
  }

  private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: AbstractRequestResponse, replicaId: Int) {
    val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
    debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
    // 响应集合
    val responseMap = stopReplicaResponse.responses.asScala
    // 整理响应中携带的每个分区的错误
    val partitionsInError =
      if (stopReplicaResponse.errorCode != Errors.NONE.code) responseMap.keySet
      else responseMap.filter { case (_, error) => error != Errors.NONE.code }.map(_._1).toSet
    // 整理得到每个副本的错误
    val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
    inLock(controllerContext.controllerLock) {
      // move all the failed replicas to ReplicaDeletionIneligible
      // 使用failReplicaDeletion()方法处理响应中有错误信息的副本
      failReplicaDeletion(replicasInError)
      // 使用completeReplicaDeletion()方法处理响应中正常的副本
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError
        completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  // 用于删除主题的线程
  class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {
    val zkUtils = controllerContext.zkUtils

    // 主要方法
    override def doWork() {
      // 等待删除操作开始
      awaitTopicDeletionNotification()

      if (!isRunning.get) // 判断状态
        return

      inLock(controllerContext.controllerLock) {

        // 收集将被删除的主题集合
        val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted

        if(!topicsQueuedForDeletion.isEmpty) // 将被删除的主题集合不为空
          info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))

        topicsQueuedForDeletion.foreach { topic => // 遍历将要被删除的主题
        // if all replicas are marked as deleted successfully, then topic deletion is done
          if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) { // 通过状态机判断主题的副本是否都删除了
            // clear up all state for this topic from controller cache and zookeeper
            // 完成主题的删除，会处理状态切换、Zookeeper相关存储及本地缓存的删除
            completeDeleteTopic(topic)
            info("Deletion of topic %s successfully completed".format(topic))
          } else {
            if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
              // 检查该主题是否存在状态为ReplicaDeletionStarted的副本，如果存在则继续等待
              // ignore since topic deletion is in progress
              // 日志打印
              val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
              val replicaIds = replicasInDeletionStartedState.map(_.replica)
              val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
              info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
                partitions.mkString(","), topic))
            } else {
              // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
              // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
              // or there is at least one failed replica (which means topic deletion should be retried).
              // 任一副本处于ReplicaDeletionIneligible状态，则重置为OfflineReplica后重试
              if(controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
                // mark topic for deletion retry
                markTopicForDeletionRetry(topic)
              }
            }
          }
          // Try delete topic if it is eligible for deletion.
          // 检测当前Topic是否可以删除
          if(isTopicEligibleForDeletion(topic)) {
            info("Deletion of topic %s (re)started".format(topic))
            // topic deletion will be kicked off
            onTopicDeletion(Set(topic)) // 开始Topic的删除操作
          } else if(isTopicIneligibleForDeletion(topic)) {
            info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
          }
        }
      }
    }
  }
}
