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

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.{ClientRequest, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, LoginType, Mode, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}
import scala.collection.mutable.HashMap

/**
  * KafkaController使用ControllerChannelManager管理其与集群中各个Broker之间的网络交互
  * @param controllerContext KafkaController上下文
  * @param config 配置信息
  * @param time
  * @param metrics
  * @param threadNamePrefix
  */
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging {
  // 用于管理集群中每个Broker对应的ControllerBrokerStateInfo对象
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  // 操作brokerStateInfo使用的锁
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  // 为Broker创建对应的ControllerBrokerStateInfo对象
  controllerContext.liveBrokers.foreach(addNewBroker(_))

  def startup() = {
    brokerLock synchronized {
      // 启动RequestSendThread线程
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  // 向指定的Broker发送请求
  def sendRequest(brokerId: Int, apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit = null) {
    brokerLock synchronized { // 加锁
      // 从brokerStateInfo获取接收请求的Broker的信息
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          // 能获取到目的Broker的信息，将发送到额请求封装为QueueItem对象放入消息队列
          stateInfo.messageQueue.put(QueueItem(apiKey, apiVersion, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  // 添加Broker以管理
  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized { // 加锁
      if(!brokerStateInfo.contains(broker.id)) { // 当前brokerStateInfo不存在该Broker
        // 使用addNewBroker()方法添加
        addNewBroker(broker)
        // 启动对应的RequestSendThread线程
        startRequestSendThread(broker.id)
      }
    }
  }

  // 移除Broker
  def removeBroker(brokerId: Int) {
    brokerLock synchronized { // 加锁
      // 获取Broker对应的ControllerBrokerStateInfo对象，然后交给removeExistingBroker()方法处理
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  // 实现对brokerStateInfo集合的管理
  private def addNewBroker(broker: Broker) {
    // 创建消息队列
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    // 从配置构建BrokerEndPoint及BrokerNode
    val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerSecurityProtocol)
    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
    // 构建网络通信组件
    val networkClient = {
      val channelBuilder = ChannelBuilders.create(
        config.interBrokerSecurityProtocol,
        Mode.CLIENT,
        LoginType.SERVER,
        config.values,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        config.connectionsMaxIdleMs,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time
      )
    }

    /**
      * 生成RequestSendThread线程的名称，格式为：
      * threadNamePrefix:Controller-源brokerId-to-broker-目的brokerId-send-thread
      */
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    // 创建RequestSendThread线程
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName)
    requestThread.setDaemon(false)
    // 将网络通信组件、Broker节点对象、消息队列和RequestSendThread线程封装为一个ControllerBrokerStateInfo对象，并放入brokerStateInfo
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
  }

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      // 关闭网络通信组件
      brokerState.networkClient.close()
      // 清空消息队列
      brokerState.messageQueue.clear()
      // 关闭RequestSendThread线程
      brokerState.requestSendThread.shutdown()
      // 从brokerStateInfo中移除对应的ControllerBrokerStateInfo对象
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  // 根据brokerId启动Broker对应的RequestSendThread线程
  protected def startRequestSendThread(brokerId: Int) {
    // 从brokerStateInfo获取brokerId对应的RequestSendThread线程
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW) // 如果该线程的状态为NEW，就将其启动
      requestThread.start()
  }
}

// 封装了Request对象和其对应的回调函数
case class QueueItem(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    // 定义线程睡眠300毫秒的方法
    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(300))

    // 从缓冲队列中获取QueueItem，并进行解析
    val QueueItem(apiKey, apiVersion, request, callback) = queue.take()
    import NetworkClientBlockingOps._
    var clientResponse: ClientResponse = null
    try {
      lock synchronized { // 加锁
        var isSendSuccessful = false
        while (isRunning.get() && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          // 当Broker宕机后，会触发Zookeeper的监听器调用removeBroker()方法将当前线程停止，在停止钱会一直尝试重试
          try {
            if (!brokerReady()) {
              isSendSuccessful = false
              // 退避一段时候后重试
              backoff()
            }
            else {
              // 构建请求头
              val requestHeader = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
              // 构建RequestSend请求数据对象
              val send = new RequestSend(brokerNode.idString, requestHeader, request.toStruct)
              // 构建ClientRequest请求对象
              val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
              // 发送请求并阻塞等待响应返回
              clientResponse = networkClient.blockingSendAndReceive(clientRequest)(time)
              // 标识标记
              isSendSuccessful = true
            }
          } catch {
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                  request.toString, brokerNode.toString()), e)
              // 发送未成功，重新连接broker并重新发送
              networkClient.close(brokerNode.idString)
              isSendSuccessful = false
              // 退避一段时间后重试
              backoff()
          }
        }
        // 处理响应
        if (clientResponse != null) {
          /**
            * 检测不同的响应构建不同的响应对象，
            * Controller只能发送LeaderAndIsrRequest、StopReplicaRequest、UpdateMetadataRequest三种请求（略）
            */
          val response = ApiKeys.forId(clientResponse.request.request.header.apiKey) match {
            case ApiKeys.LEADER_AND_ISR => new LeaderAndIsrResponse(clientResponse.responseBody)
            case ApiKeys.STOP_REPLICA => new StopReplicaResponse(clientResponse.responseBody)
            case ApiKeys.UPDATE_METADATA_KEY => new UpdateMetadataResponse(clientResponse.responseBody)
            case apiKey => throw new KafkaException(s"Unexpected apiKey received: $apiKey")
          }
          stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
            .format(controllerId, controllerContext.epoch, response.toString, brokerNode.toString))

          if (callback != null) {
            // 调用QueueItem中封装的回调函数
            callback(response)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString()), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    import NetworkClientBlockingOps._
    try {

      // 检查Broker节点是否已经准备好
      if (networkClient.isReady(brokerNode, time.milliseconds()))
        true
      else {
        // 阻塞等待结果
        val ready = networkClient.blockingReady(brokerNode, socketTimeoutMs)(time)

        if (!ready)
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString()))
        true
      }
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString()), e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

}


/**
  * ControllerBrokerRequestBatch组件用于实现批量发送请求的功能
  * @param controller
  */
class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  // 记录了发往指定Broker的LeaderAndIsrRequest所需的信息
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  // 记录了发往指定Broker的StopReplicaRequest所需的信息
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  // 记录了发往指定Broker的UpdateMetadataRequest集合
  val updateMetadataRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  // 用于检测leaderAndIsrRequestMap、stopReplicaRequestMap、updateMetadataRequestMap，有一个不为空就会抛出异常
  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
  }

  // 清空leaderAndIsrRequestMap、stopReplicaRequestMap、updateMetadataRequestMap
  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestMap.clear()
  }

  /**
    * 向leaderAndIsrRequestMap集合中添加待发送的LeaderAndIsrRequest所需的数据，
    * 同时调用addUpdateMetadataRequestForBrokers()方法准备向集群中所有可用的Broker发送UpdateMetadataRequest
    * @param brokerIds 接收LeaderAndIsrRequest的Broker的ID集合
    * @param topic 主题
    * @param partition 分区
    * @param leaderIsrAndControllerEpoch 年代信息
    * @param replicas 新分配的副本集
    * @param callback 回调
    */
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: AbstractRequestResponse => Unit = null) {

    // 通过主题和分区构造TopicPartition对象
    val topicPartition = new TopicPartition(topic, partition)

    // 对Broker的ID集合进行有效性过滤，然后进行遍历
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      // 从leaderAndIsrRequestMap获取发往指定Broker的LeaderAndIsrRequest所需的信息
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      // 添加Leader、ISR、AR等构造LeaderAndIsrRequest请求所需要的信息
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }

    // 准备向所有可用的Broker发送UpdateMetadataRequest请求
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }

  // 向stopReplicaRequestMap集合添加StopReplicaRequest请求
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractRequestResponse, Int) => Unit = null) {
    // 对Broker的ID集合进行有效性过滤，然后进行遍历
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      // 从stopReplicaRequestMap获取发往Broker的StopReplicaRequestInfo集合，如果获取不到就更新为空集合
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      // 获取发往Broker的StopReplicaRequestInfo集合v
      val v = stopReplicaRequestMap(brokerId)
      // 根据回调情况构造StopReplicaRequestInfo并添加到集合v中
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractRequestResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /**
    * Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted
    * 向给定的Broker发送UpdateMetadataRequest请求
    * */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: AbstractRequestResponse => Unit = null) {
    // 定义回调函数
    def updateMetadataRequestMapFor(partition: TopicAndPartition, beingDeleted: Boolean) {
      // 找出Controller中保存的该分区的Leader
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          // 获取分区的AR集合
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          // 根据beingDeleted参数，构造PartitionStateInfo对象
          val partitionStateInfo = if (beingDeleted) {
            val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
            PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
          } else {
            PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          }
          // 对Broker的ID集合进行有效性过滤，然后进行遍历，向updateMetadataRequestMap中添加数据
          brokerIds.filter(b => b >= 0).foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
          }
        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    val filteredPartitions = {
      val givenPartitions = if (partitions.isEmpty)
        // 如果给定的Partition集合为空，返回空键集
        controllerContext.partitionLeadershipInfo.keySet
      else
        partitions
      if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        // 过滤即将被删除的Partition
        givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
    }
    if (filteredPartitions.isEmpty) // 最后得到的Partition集合为空
      // 更新updateMetadataRequestMap，每个BrokerID对应一个空的字典
      brokerIds.filter(b => b >= 0).foreach { brokerId =>
        updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
      }
    else // 最后得到的Partition集合不为空
      // 调用updateMetadataRequestMapFor()方法将filteredPartitions中的分区信息添加到updateMetadataRequestMap集合中，等待发送
      filteredPartitions.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = false))

    // 将即将被删除的分区信息添加到updateMetadataRequestMap集合中，等待发送，beingDeleted参数为true
    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = true))
  }

  /**
    * 根据leaderAndIsrRequestMap、stopReplicaRequestMap、updateMetadataRequestMap中的数据创建对应的请求
    * 并添加到ControllerChannelManager中对应的消息队列中，最终由RequestSendThread线程发送这些请求
    */
  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      // 遍历leaderAndIsrRequestMap集合
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        // 日志记录Leader和Follower分配的情况
        partitionStateInfos.foreach { case (topicPartition, state) =>
          val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                   "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition))
        }
        // Leader角色的ID集合
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
        // 从可用Broker集合中过滤得到Leader角色所属的Node节点对象
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerSecurityProtocol)
        }
        // 根据Leader角色集合构建对应的Map[TopicPartition, PartitionState]对象
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          // 解析得到Leader的ID、年代信息、ISR集合及Controller年代信息
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          // 构建PartitionState对象
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          topicPartition -> partitionState
        }
        // 创建LeaderAndIsrRequest请求对象
        val leaderAndIsrRequest = new LeaderAndIsrRequest(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
        // 使用KafkaController发送请求
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, None, leaderAndIsrRequest, null)
      }
      // 清空leaderAndIsrRequestMap集合
      leaderAndIsrRequestMap.clear()

      // 遍历updateMetadataRequestMap集合
      updateMetadataRequestMap.foreach { case (broker, partitionStateInfos) =>
        // 遍历每个Broker对应的Map[TopicPartition, PartitionStateInfo]字典，打印日志
        partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
          "to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
          broker, p._1)))
        // 为每个Broker对应的Map[TopicPartition, PartitionStateInfo]字典构建对应的Map[TopicPartition, PartitionState]对象
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          topicPartition -> partitionState
        }

        // Kafka版本判断
        val version = if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2: Short
                      else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1: Short
                      else 0: Short

        // 根据Kafka版本构建UpdateMetadataRequest请求对象
        val updateMetadataRequest =
          if (version == 0) {
            // 可用Broker集合
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map(_.getNode(SecurityProtocol.PLAINTEXT))
            // 构建UpdateMetadataRequest请求对象
            new UpdateMetadataRequest(controllerId, controllerEpoch, liveBrokers.asJava, partitionStates.asJava)
          }
          else {
            // 遍历可用Broker集合
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map { broker =>
              // 得到每个Broker的EndPoint
              val endPoints = broker.endPoints.map { case (securityProtocol, endPoint) =>
                securityProtocol -> new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port)
              }
              // 根据BrokerID、EndPoint、Broker的机架信息构建对应的Broker对象
              new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
            }
            // 构建UpdateMetadataRequest请求对象
            new UpdateMetadataRequest(version, controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava)
          }

        // 使用KafkaController发送请求
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, Some(version), updateMetadataRequest, null)
      }
      // 清空updateMetadataRequestMap集合
      updateMetadataRequestMap.clear()

      // 遍历stopReplicaRequestMap集合
      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        // 日志记录被删除和保留的副本集合
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))
        replicaInfoList.foreach { r =>
          // 构建StopReplicaRequest请求
          val stopReplicaRequest = new StopReplicaRequest(controllerId, controllerEpoch, r.deletePartition,
            Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          // 使用KafkaController发送请求
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, None, stopReplicaRequest, r.callback)
        }
      }
      // 清空stopReplicaRequestMap集合
      stopReplicaRequestMap.clear()
    } catch {
      case e : Throwable => {
        if (leaderAndIsrRequestMap.size > 0) {
          error("Haven't been able to send leader and isr requests, current state of " +
              s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestMap.size > 0) {
          error("Haven't been able to send metadata update requests, current state of " +
              s"the map is $updateMetadataRequestMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.size > 0) {
          error("Haven't been able to send stop replica requests, current state of " +
              s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
      }
    }
  }
}

/**
  * 表示ControllerChannelManager与一个Broker连接的各种信息
  * @param networkClient 维护Controller与对应Broker通信的网络连接，使用NetworkClientBlockingOps配合实现的阻塞方式
  * @param brokerNode 维护了对应的Broker的网络位置信息，其中记录了Broker的host、ip、port以及机架信息
  * @param messageQueue 缓冲队列（LinkedBlockingQueue类型），存放了发往对应Broker的请求，其中每个元素是QueueItem类型，其中封装了Request本身和其对应的回调函数
  * @param requestSendThread 用于发送请求的线程
  */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread)

/**
  * @param replica 分区及副本信息
  * @param deletePartition 是否删除分区
  * @param callback 回调
  */
case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractRequestResponse => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback: AbstractRequestResponse => Unit = null,
                         var updateMetadataResponseCallback: AbstractRequestResponse => Unit = null,
                         var stopReplicaResponseCallback: (AbstractRequestResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk: AbstractRequestResponse => Unit = null
    var updateMetadataResponseCbk: AbstractRequestResponse => Unit = null
    var stopReplicaResponseCbk: (AbstractRequestResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (AbstractRequestResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
