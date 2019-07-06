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

import java.nio.ByteBuffer
import java.lang.{Long => JLong, Short => JShort}
import java.util.Properties

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api._
import kafka.cluster.Partition
import kafka.common
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.{GroupCoordinator, JoinGroupResult}
import kafka.log._
import kafka.message.{ByteBufferMessageSet, Message, MessageSet}
import kafka.network._
import kafka.network.RequestChannel.{Response, Session}
import kafka.security.auth.{Authorizer, ClusterAction, Create, Describe, Group, Operation, Read, Resource, Topic, Write}
import kafka.utils.{Logging, SystemTime, ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidTopicException, NotLeaderForPartitionException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol, SecurityProtocol}
import org.apache.kafka.common.requests.{ApiVersionsResponse, DescribeGroupsRequest, DescribeGroupsResponse, GroupCoordinatorRequest, GroupCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, LeaveGroupRequest, LeaveGroupResponse, ListGroupsResponse, ListOffsetRequest, ListOffsetResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse, ResponseHeader, ResponseSend, StopReplicaRequest, StopReplicaResponse, SyncGroupRequest, SyncGroupResponse, UpdateMetadataRequest, UpdateMetadataResponse}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.internals.TopicConstants

import scala.collection._
import scala.collection.JavaConverters._
import org.apache.kafka.common.requests.SaslHandshakeResponse

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val coordinator: GroupCoordinator,
                val controller: KafkaController,
                val zkUtils: ZkUtils,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer]) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  // Store all the quota managers for each type of request
  val quotaManagers: Map[Short, ClientQuotaManager] = instantiateQuotaManagers(config)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try {
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      // 根据requestId获取请求对应的ApiKeys，进行匹配
      ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProducerRequest(request) // 生产
        case ApiKeys.FETCH => handleFetchRequest(request) // 拉取
        case ApiKeys.LIST_OFFSETS => handleOffsetRequest(request) // 获取offsets
        case ApiKeys.METADATA => handleTopicMetadataRequest(request) // 获取元数据（由生产者或消费者客户端向服务端获取集群元数据）
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request) // 操作Leader及ISR信息，LeaderAndIsrRequest
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request) // 停止副本，可能会删除副本
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request) // 更新元数据（由KafkaController要求Broker更新）
        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request) // 提交offset，OffsetCommitRequest
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request) // 获取offset，OffsetFetchRequest
        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request) // 获取GroupCoordinator，GroupCoordinatorRequest
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request) // JoinGroup请求，JoinGroupRequest
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request) // 心跳，HeartbeatRequest
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request) // 离开Group，LeaveGroupRequest
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request) // 同步Group，SyncGroupRequest
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request) // 获取Group信息
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request) // 获取Group列表，ListGroupsRequest
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request) // SASL握手
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request) // 获取API Version
        case requestId => throw new KafkaException("Unknown api code " + requestId) // 未知
      }
    } catch {
      case e: Throwable =>
        if (request.requestObj != null) {
          // 如果request.requestObj不为空，则使用request.requestObj处理异常
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          // 否则构建Response，使用RequestChannel返回异常响应
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  // 用于操作Leader或ISR的请求
  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    // 转换请求为LeaderAndIsrRequest
    val leaderAndIsrRequest = request.body.asInstanceOf[LeaderAndIsrRequest]

    try {
      def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
        // for each new leader or follower, call coordinator to handle consumer group migration.
        // this callback is invoked under the replica state change lock to ensure proper order of
        // leadership changes
        // 处理GroupCoordinator的迁移
        updatedLeaders.foreach { partition =>
          if (partition.topic == TopicConstants.GROUP_METADATA_TOPIC_NAME) // 主题为__consumer_offsets
            // Broker成为Offsets Topic分区的Leader副本时调用
            coordinator.handleGroupImmigration(partition.partitionId)
        }
        updatedFollowers.foreach { partition =>
          if (partition.topic == TopicConstants.GROUP_METADATA_TOPIC_NAME) // 主题为__consumer_offsets
          // Broker成为Offsets Topic分区的Follower副本时调用
            coordinator.handleGroupEmigration(partition.partitionId)
        }
      }

      // 根据correlationId构造响应头，请求端会根据响应的correlationId以匹配发送的请求对象
      val responseHeader = new ResponseHeader(correlationId)
      val leaderAndIsrResponse =
        if (authorize(request.session, ClusterAction, Resource.ClusterResource)) { // 检查授权
          // 授权通过，调用ReplicaManager的becomeLeaderOrFollower(...)方法进行处理，注意此处会传入上面定义的onLeadershipChange回调方法
          val result = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, metadataCache, onLeadershipChange)
          // 将处理的结果构造为LeaderAndIsrResponse响应对象
          new LeaderAndIsrResponse(result.errorCode, result.responseMap.mapValues(new JShort(_)).asJava)
        } else {
          // 授权未通过，向leaderAndIsrRequest中记录CLUSTER_AUTHORIZATION_FAILED错误码
          val result = leaderAndIsrRequest.partitionStates.asScala.keys.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
          // 构造LeaderAndIsrResponse响应，错误码为CLUSTER_AUTHORIZATION_FAILED
          new LeaderAndIsrResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
        }

      // 使用RequestChannel发送响应
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    // 把请求转换为StopReplicaRequest
    val stopReplicaRequest = request.body.asInstanceOf[StopReplicaRequest]

    // 构造响应头
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val response =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) { // 检查授权
        // 使用ReplicaManager处理请求
        val (result, error) = replicaManager.stopReplicas(stopReplicaRequest)
        // 构造StopReplicaResponse响应对象
        new StopReplicaResponse(error, result.asInstanceOf[Map[TopicPartition, JShort]].asJava)
      } else {
        // 授权失败，将响应结果中的分区的错误码设置为CLUSTER_AUTHORIZATION_FAILED
        val result = stopReplicaRequest.partitions.asScala.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
        new StopReplicaResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
      }

    // 使用RequestChannel发送响应
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
    // 停止空闲的副本同步线程
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  // 处理更新Broker元数据的请求
  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    // 请求关联ID
    val correlationId = request.header.correlationId
    // 将请求转换为UpdateMetadataRequest
    val updateMetadataRequest = request.body.asInstanceOf[UpdateMetadataRequest]

    val updateMetadataResponse =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) { // 检查授权
        // 使用ReplicaManager更新
        replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest, metadataCache)
        // 构造错误码为NONE的响应
        new UpdateMetadataResponse(Errors.NONE.code)
      } else {
        // 未授权，直接返回错误码为CLUSTER_AUTHORIZATION_FAILED的响应
        new UpdateMetadataResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
      }

    // 构造响应头
    val responseHeader = new ResponseHeader(correlationId)
    // 使用RequestChannel发送请求
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]

    authorizeClusterAction(request)

    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      Errors.NONE.code, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
  }


  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    // 获取请求头、转换请求对象类型
    val header = request.header
    val offsetCommitRequest = request.body.asInstanceOf[OffsetCommitRequest]

    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetCommitRequest.groupId))) { // 检查授权
      val errorCode = new JShort(Errors.GROUP_AUTHORIZATION_FAILED.code)
      val results = offsetCommitRequest.offsetData.keySet.asScala.map { topicPartition =>
        (topicPartition, errorCode)
      }.toMap
      val responseHeader = new ResponseHeader(header.correlationId)
      val responseBody = new OffsetCommitResponse(results.asJava)
      // 授权失败的响应返回
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // filter non-existent topics
      // 过滤不存在的主题
      val invalidRequestsInfo = offsetCommitRequest.offsetData.asScala.filter { case (topicPartition, _) =>
        !metadataCache.contains(topicPartition.topic)
      }
      val filteredRequestInfo = offsetCommitRequest.offsetData.asScala.toMap -- invalidRequestsInfo.keys

      // 授权检查是否允许操作目标主题
      val (authorizedRequestInfo, unauthorizedRequestInfo) = filteredRequestInfo.partition {
        case (topicPartition, offsetMetadata) => authorize(request.session, Read, new Resource(Topic, topicPartition.topic))
      }

      // the callback for sending an offset commit response
      // 回调方法
      def sendResponseCallback(commitStatus: immutable.Map[TopicPartition, Short]) {
        val mergedCommitStatus = commitStatus ++ unauthorizedRequestInfo.mapValues(_ => Errors.TOPIC_AUTHORIZATION_FAILED.code)

        mergedCommitStatus.foreach { case (topicPartition, errorCode) =>
          if (errorCode != Errors.NONE.code) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${Errors.forCode(errorCode).exceptionName}")
          }
        }
        val combinedCommitStatus = mergedCommitStatus.mapValues(new JShort(_)) ++ invalidRequestsInfo.map(_._1 -> new JShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))

        val responseHeader = new ResponseHeader(header.correlationId)
        val responseBody = new OffsetCommitResponse(combinedCommitStatus.asJava)
        requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
      }

      if (authorizedRequestInfo.isEmpty) // 通过授权的请求为空，直接调用回调方法，传入空集合参数
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) { // apiVersion为0的情况，需要写入到Zookeeper中，新版本才是由主题__consumer_offsets进行管理
        // for version 0 always store offsets to ZK
        val responseInfo = authorizedRequestInfo.map { // 遍历处理通过授权的请求，最终获得处理结果
          case (topicPartition, partitionData) =>
            // 构造得到封装了/consumers/[group_id]/offsets/[topic_name]和/consumers/[group_id]/owners/[topic_name]路径的ZKGroupTopicDirs对象
            val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicPartition.topic)
            try {
              if (!metadataCache.hasTopicMetadata(topicPartition.topic)) // 元数据不包含该主题
                (topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code) // 返回UNKNOWN_TOPIC_OR_PARTITION错误
              else if (partitionData.metadata != null && // 对应分区的元数据存在
                        partitionData.metadata.length > config.offsetMetadataMaxSize) // 分区的元数据过大（offset.metadata.max.bytes）
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE.code) // 返回OFFSET_METADATA_TOO_LARGE错误
              else { // 正常情况
                // 更新Zookeeper中/consumers/[group_id]/offsets/[topic_name]/[partition_id]的数据为提交的Offset
                zkUtils.updatePersistentPath(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}", partitionData.offset.toString)
                (topicPartition, Errors.NONE.code) // 无错误码返回
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e).code)
            }
        }
        // 执行回调
        sendResponseCallback(responseInfo)
      } else { // apiVersion不为0的情况
        // for version 1 and beyond store offsets in offset manager

        // compute the retention time based on the request version:
        // if it is v1 or not specified by user, we can use the default retention
        // apiVersion <= 1或者没有指定offset的保留时长，使用默认时长（24小时）
        val offsetRetention =
          if (header.apiVersion <= 1 ||
            offsetCommitRequest.retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME) // -1
            coordinator.offsetConfig.offsetsRetentionMs // 默认为24*60*60*1000L
          else
            offsetCommitRequest.retentionTime

        // commit timestamp is always set to now.
        // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
        // expire timestamp is computed differently for v1 and v2.
        //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
        //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
        //   - If v2 we use the default expiration timestamp
        // 时间戳
        val currentTimestamp = SystemTime.milliseconds
        // 过期时间
        val defaultExpireTimestamp = offsetRetention + currentTimestamp
        // 遍历请求进行格式处理，转换为OffsetAndMetadata对象
        val partitionData = authorizedRequestInfo.mapValues { partitionData =>
          val metadata = if (partitionData.metadata == null) OffsetMetadata.NoMetadata else partitionData.metadata;
          new OffsetAndMetadata(
            // 消息数据
            offsetMetadata = OffsetMetadata(partitionData.offset, metadata),
            // 提交时间
            commitTimestamp = currentTimestamp,
            // 过期时间
            expireTimestamp = {
              if (partitionData.timestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP)
                defaultExpireTimestamp
              else
                offsetRetention + partitionData.timestamp
            }
          )
        }

        // call coordinator to handle commit offset
        // 使用GroupCoordinator处理提交的offset消息
        coordinator.handleCommitOffsets(
          offsetCommitRequest.groupId,
          offsetCommitRequest.memberId,
          offsetCommitRequest.generationId,
          partitionData,
          sendResponseCallback)
      }
    }
  }

  private def authorize(session: Session, operation: Operation, resource: Resource): Boolean =
    authorizer.map(_.authorize(session, operation, resource)).getOrElse(true)

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    // 获取请求体并转换为ProduceRequest对象
    val produceRequest = request.body.asInstanceOf[ProduceRequest]
    // 计算需要添加的字节数
    val numBytesAppended = request.header.sizeOf + produceRequest.sizeOf

    // 检查ProducerRequest请求定义的分区是否授权有写操作
    val (authorizedRequestInfo, unauthorizedRequestInfo) = produceRequest.partitionRecords.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Write, new Resource(Topic, topicPartition.topic))
    }

    // the callback for sending a produce response
    /**
      * 这个函数会当做回调函数最终传递给DelayedProduce的responseCallback参数
      * @param responseStatus
      */
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      // 生成响应状态集合，其中包括通过授权验证并处理完成的状态（responseStatus），以及未通过授权验证的状态（unauthorizedRequestInfo）
      val mergedResponseStatus = responseStatus ++ unauthorizedRequestInfo.mapValues(_ =>
        new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, Message.NoTimestamp))

      // 标识处理ProducerRequest的过程中是否出现异常
      var errorInResponse = false
      // 遍历响应状态集合
      mergedResponseStatus.foreach { case (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) { // 当存在错误状态码时
          // 将errorInResponse置为true
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      // 定义produceResponseCallback()回调函数
      def produceResponseCallback(delayTimeMs: Int) {
        // 处理acks字段为0的情况，即生产者不需要服务端确认ACK
        if (produceRequest.acks == 0) {
          // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
          // the request, since no response is expected by the producer, the server will close socket server so that
          // the producer client will know that some error has happened and will refresh its metadata
          if (errorInResponse) {
            val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
              topicPartition -> Errors.forCode(status.errorCode).exceptionName
            }.mkString(", ")
            info(
              s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
                s"from client id ${request.header.clientId} with ack=0\n" +
                s"Topic and partition to exceptions: $exceptionsSummary"
            )
            /**
              * 处理ProducerRequest过程中出现异常，
              * 则向对应的responseQueue中添加RequestChannel.CloseConnectionAction类型的响应，关闭连接
              */
            requestChannel.closeConnection(request.processor, request)
          } else {
            /**
              * 处理ProducerRequest过程中未出现异常，
              * 则向对应的responseQueue中添加RequestChannel.NoOpAction类型的响应，继续读取客户端的请求
              */
            requestChannel.noOperation(request.processor, request)
          }
        } else { // 处理acks字段为1或-1的情况，即生产者需要服务端确认ACK
          // 创建消息头
          val respHeader = new ResponseHeader(request.header.correlationId)
          // 创建消息体
          val respBody = request.header.apiVersion match {
            case 0 => new ProduceResponse(mergedResponseStatus.asJava)
            case version@(1 | 2) => new ProduceResponse(mergedResponseStatus.asJava, delayTimeMs, version)
            // This case shouldn't happen unless a new version of ProducerRequest is added without
            // updating this part of the code to handle it properly.
            case version => throw new IllegalArgumentException(s"Version `$version` of ProduceRequest is not handled. Code must be updated.")
          }
          // 向对应的responseQueue中添加RequestChannel.SendAction类型的响应，将响应返回给客户端
          requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, respBody)))
        }
      }

      // When this callback is triggered, the remote API call has completed
      // 设置API调用的远程完成时间为当前时间
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds
      // 记录监控数据，但在其内部会调用produceResponseCallback()回调函数
      quotaManagers(ApiKeys.PRODUCE.id).recordAndMaybeThrottle(
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)
    }

    if (authorizedRequestInfo.isEmpty)
      // 如果ProducerRequest请求的所有分区都无写授权，就直接调用sendResponseCallback()回调，无响应数据
      sendResponseCallback(Map.empty)
    else {
      // 决定是否可以操作内部主题，只有发出请求的客户端ID是"__admin_client"才可以操作内部主题
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // Convert ByteBuffer to ByteBufferMessageSet
      // 将授权的ProducerRequest请求写入数据转换为ByteBufferMessageSet对象
      val authorizedMessagesPerPartition = authorizedRequestInfo.map {
        case (topicPartition, buffer) => (topicPartition, new ByteBufferMessageSet(buffer))
      }

      // call the replica manager to append messages to the replicas
      // 使用ReplicaManager的appendMessages(...)方法添加数据
      replicaManager.appendMessages(
        produceRequest.timeout.toLong,
        produceRequest.acks,
        internalTopicsAllowed,
        authorizedMessagesPerPartition,
        sendResponseCallback)

      // if the request is put into the purgatory, it will have a held reference
      // and hence cannot be garbage collected; hence we clear its data here in
      // order to let GC re-claim its memory since it is already appended to log
      // 将ProducerRequest内部数据清除以进行GC
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    // 转换请求为FetchRequest对象
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    // 验证主题分区授权情况
    val (authorizedRequestInfo, unauthorizedRequestInfo) = fetchRequest.requestInfo.partition {
      case (topicAndPartition, _) => authorize(request.session, Read, new Resource(Topic, topicAndPartition.topic))
    }
    // 处理未授权主题分区应该返回的数据
    val unauthorizedPartitionData = unauthorizedRequestInfo.mapValues { _ =>
      FetchResponsePartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, MessageSet.Empty)
    }

    // the callback for sending a fetch response
    // 该Callback函数最后会传递给DelayedFetch对象作为其responseCallback参数
    def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
      // 协议版本转换
      val convertedPartitionData =
        // Need to down-convert message when consumer only takes magic value 0.
        if (fetchRequest.versionId <= 1) {
          responsePartitionData.map { case (tp, data) =>

            // We only do down-conversion when:
            // 1. The message format version configured for the topic is using magic value > 0, and
            // 2. The message set contains message whose magic > 0
            // This is to reduce the message format conversion as much as possible. The conversion will only occur
            // when new message format is used for the topic and we see an old request.
            // Please note that if the message format is changed from a higher version back to lower version this
            // test might break because some messages in new message format can be delivered to consumers before 0.10.0.0
            // without format down conversion.
            val convertedData = if (replicaManager.getMessageFormatVersion(tp).exists(_ > Message.MagicValue_V0) &&
              !data.messages.isMagicValueInAllWrapperMessages(Message.MagicValue_V0)) {
              trace(s"Down converting message to V0 for fetch request from ${fetchRequest.clientId}")
              new FetchResponsePartitionData(data.error, data.hw, data.messages.asInstanceOf[FileMessageSet].toMessageFormat(Message.MagicValue_V0))
            } else data

            tp -> convertedData
          }
        } else responsePartitionData

      // 将之前把未认证通过的集合与convertedPartitionData合并
      val mergedPartitionData = convertedPartitionData ++ unauthorizedPartitionData

      // 记录错误日志
      mergedPartitionData.foreach { case (topicAndPartition, data) =>
        if (data.error != Errors.NONE.code)
          debug(s"Fetch request with correlation id ${fetchRequest.correlationId} from client ${fetchRequest.clientId} " +
            s"on partition $topicAndPartition failed due to ${Errors.forCode(data.error).exceptionName}")
        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesOutRate.mark(data.messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.messages.sizeInBytes)
      }

      // 用于发送响应
      def fetchResponseCallback(delayTimeMs: Int) {
        trace(s"Sending fetch response to client ${fetchRequest.clientId} of " +
          s"${convertedPartitionData.values.map(_.messages.sizeInBytes).sum} bytes")
        // 生成FetchResponse对象
        val response = FetchResponse(fetchRequest.correlationId, mergedPartitionData, fetchRequest.versionId, delayTimeMs)
        // 向对应responseQueue中添加一个SendAction的Response，其中封装了上面的FetchResponse对象
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, response)))
      }


      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds

      // Do not throttle replication traffic
      if (fetchRequest.isFromFollower) {
        // 调用fetchResponseCallback()返回FetchResponse
        fetchResponseCallback(0)
      } else {
        // 底层调用fetchResponseCallback()
        quotaManagers(ApiKeys.FETCH.id).recordAndMaybeThrottle(fetchRequest.clientId,
                                                               FetchResponse.responseSize(mergedPartitionData.groupBy(_._1.topic),
                                                                                          fetchRequest.versionId),
                                                               fetchResponseCallback)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      // 没有通过授权的主题分区，直接返回空字典
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      // 有通过授权的主题分区，使用ReplicaManager进行处理
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        authorizedRequestInfo,
        sendResponseCallback)
    }
  }

  /**
   * Handle an offset request
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body.asInstanceOf[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.offsetData.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, List[JLong]().asJava)
    )

    val responseMap = authorizedRequestInfo.map(elem => {
      val (topicPartition, partitionData) = elem
      try {
        // ensure leader exists
        val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
          replicaManager.getLeaderReplicaIfLocal(topicPartition.topic, topicPartition.partition)
        else
          replicaManager.getReplicaOrException(topicPartition.topic, topicPartition.partition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicPartition,
                                        partitionData.timestamp,
                                        partitionData.maxNumOffsets)
          if (offsetRequest.replicaId != ListOffsetRequest.CONSUMER_REPLICA_ID) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE.code, offsets.map(new JLong(_)).asJava))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition, utpe.getMessage))
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(utpe).code, List[JLong]().asJava))
        case nle: NotLeaderForPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition,nle.getMessage))
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(nle).code, List[JLong]().asJava))
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e).code, List[JLong]().asJava))
      }
    })

    val mergedResponseMap = responseMap ++ unauthorizedResponseStatus

    val responseHeader = new ResponseHeader(correlationId)
    val response = new ListOffsetResponse(mergedResponseMap.asJava)

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
  }

  def fetchOffsets(logManager: LogManager, topicPartition: TopicPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(TopicAndPartition(topicPartition.topic, topicPartition.partition)) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP || timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
          Seq(0L)
        else
          Nil
    }
  }

  private[server] def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray
    var offsetTimeArray: Array[(Long, Long)] = null
    val lastSegmentHasSize = segsArray.last.size > 0
    if (lastSegmentHasSize)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for (i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
  }

  // 创建主题
  private def createTopic(topic: String,
                          numPartitions: Int,
                          replicationFactor: Int,
                          properties: Properties = new Properties()): MetadataResponse.TopicMetadata = {
    try {
      // 使用AdminUtils创建主题
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, properties, RackAwareMode.Safe)
      info("Auto creation of topic %s with %d partitions and replication factor %d is successful"
        .format(topic, numPartitions, replicationFactor))
      new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, common.Topic.isInternal(topic),
        java.util.Collections.emptyList())
    } catch {
      case e: TopicExistsException => // let it go, possibly another broker created this topic
        new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, common.Topic.isInternal(topic),
          java.util.Collections.emptyList())
      case itex: InvalidTopicException =>
        new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION, topic, common.Topic.isInternal(topic),
          java.util.Collections.emptyList())
    }
  }

  // 创建Group组的元数据存放的主题
  private def createGroupMetadataTopic(): MetadataResponse.TopicMetadata = {
    val aliveBrokers = metadataCache.getAliveBrokers
    val offsetsTopicReplicationFactor =
      if (aliveBrokers.nonEmpty)
        // offsetsTopicReplicationFactor由offsets.topic.replication.factor参数配置
        Math.min(config.offsetsTopicReplicationFactor.toInt, aliveBrokers.length)
      else
        config.offsetsTopicReplicationFactor.toInt
    // 创建__consumer_offsets
    createTopic(TopicConstants.GROUP_METADATA_TOPIC_NAME, // __consumer_offsets
      config.offsetsTopicPartitions, // offsets.topic.num.partitions
      offsetsTopicReplicationFactor,
      coordinator.offsetsTopicConfigs)
  }

  // 获取存储Consumer Group中消费者消费的offset的主题__consumer_offsets，如果获取不到就创建
  private def getOrCreateGroupMetadataTopic(securityProtocol: SecurityProtocol): MetadataResponse.TopicMetadata = {
    val topicMetadata = metadataCache.getTopicMetadata(Set(TopicConstants.GROUP_METADATA_TOPIC_NAME), securityProtocol)
    topicMetadata.headOption.getOrElse(createGroupMetadataTopic())
  }

  // 获取指定主题集合的元数据
  private def getTopicMetadata(topics: Set[String], securityProtocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    // 通过MetadataCache获取主题元数据
    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol, errorUnavailableEndpoints)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      // 处理未知的主题
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == TopicConstants.GROUP_METADATA_TOPIC_NAME) {
          // 创建Group组的元数据存放的主题__consumer_offsets
          createGroupMetadataTopic()
        } else if (config.autoCreateTopicsEnable) { // auto.create.topics.enable
          // 创建其他未知主题
          createTopic(topic, config.numPartitions, config.defaultReplicationFactor)
        } else {
          // 无法创建未知主题
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, common.Topic.isInternal(topic),
            java.util.Collections.emptyList())
        }
      }
      topicResponses ++ responsesForNonExistentTopics
    }
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    // 转换请求为MetadataRequest
    val metadataRequest = request.body.asInstanceOf[MetadataRequest]
    // 获取API版本
    val requestVersion = request.header.apiVersion()

    val topics =
      // Handle old metadata request logic. Version 0 has no way to specify "no topics".
      if (requestVersion == 0) {
        // 请求的topics字段为空
        if (metadataRequest.topics() == null || metadataRequest.topics().isEmpty)
          // 则读取所有的Topic信息
          metadataCache.getAllTopics()
        else
          // 否则从请求中得到需要获取信息的Topic
          metadataRequest.topics.asScala.toSet
      } else {
        if (metadataRequest.isAllTopics) // 内部实现为topics == null，即所请求的topics字段为空
          // 则读取所有的Topic信息
          metadataCache.getAllTopics()
        else
          // 否则从请求中得到需要获取信息的Topic
          metadataRequest.topics.asScala.toSet
      }

    // 授权验证，将主题分为授权主题和未授权主题
    var (authorizedTopics, unauthorizedTopics) =
      topics.partition(topic => authorize(request.session, Describe, new Resource(Topic, topic)))

    if (authorizedTopics.nonEmpty) {
      // 过滤出元数据中没有的已授权的主题
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
      // 如果开启了自动创建主题
      if (config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        authorizer.foreach { az =>
          // 验证授权，授权不通过就将这些主题从授权集合移动到未授权集合
          if (!az.authorize(request.session, Create, Resource.ClusterResource)) {
            authorizedTopics --= nonExistingTopics
            unauthorizedTopics ++= nonExistingTopics
          }
        }
      }
    }

    // 对未授权的主题统一构建带有TOPIC_AUTHORIZATION_FAILED错误码的TopicMetadata对象
    val unauthorizedTopicMetadata = unauthorizedTopics.map(topic =>
      new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, common.Topic.isInternal(topic),
        java.util.Collections.emptyList()))

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    /**
      * 根据请求版本决定是否要对副本不可用的broker返回错误
      * 1. 在版本0中，副本不可用的broker会返回错误；
      * 2. 高版本中仅仅会在返回的broker列表中去除副本不可用的broker，但不会返回错误
      */
    val errorUnavailableEndpoints = requestVersion == 0
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        // 如果已授权主题集合为空，返回空字典
        Seq.empty[MetadataResponse.TopicMetadata]
      else
        // 否则获取已授权主题的元数据
        getTopicMetadata(authorizedTopics, request.securityProtocol, errorUnavailableEndpoints)

    val completeTopicMetadata = topicMetadata ++ unauthorizedTopicMetadata

    // 获取可用broker
    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    // 创建与Request关联的响应头
    val responseHeader = new ResponseHeader(request.header.correlationId)

    // 创建响应体
    val responseBody = new MetadataResponse(
      brokers.map(_.getNode(request.securityProtocol)).asJava,
      metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
      completeTopicMetadata.asJava,
      requestVersion
    )
    // 使用RequestChannel返回响应
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  /*
   * Handle an offset fetch request
   *
   * Consumer Group向GroupCoordinator发送OffsetFetchRequest请求
   * 以获取最近一次提交的offset
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    // 获取请求头，转换请求为OffsetFetchRequest对象
    val header = request.header
    val offsetFetchRequest = request.body.asInstanceOf[OffsetFetchRequest]

    // 构造响应头
    val responseHeader = new ResponseHeader(header.correlationId)
    val offsetFetchResponse =
    // reject the request if not authorized to the group
    // 检查Group ID的授权
    if (!authorize(request.session, Read, new Resource(Group, offsetFetchRequest.groupId))) { // 检查授权未通过
      // 构造封装了INVALID_OFFSET和GROUP_AUTHORIZATION_FAILED的响应对象
      val unauthorizedGroupResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_AUTHORIZATION_FAILED.code)
      val results = offsetFetchRequest.partitions.asScala.map { topicPartition => (topicPartition, unauthorizedGroupResponse)}.toMap
      new OffsetFetchResponse(results.asJava)
    } else {
      // 检查分区的授权情况
      val (authorizedTopicPartitions, unauthorizedTopicPartitions) = offsetFetchRequest.partitions.asScala.partition { topicPartition =>
        authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
      }

      // 构造未通过授权的分区的响应集合
      val unauthorizedTopicResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.TOPIC_AUTHORIZATION_FAILED.code)
      val unauthorizedStatus = unauthorizedTopicPartitions.map(topicPartition => (topicPartition, unauthorizedTopicResponse)).toMap

      // 未知分区的响应，包含INVALID_OFFSET和UNKNOWN_TOPIC_OR_PARTITION错误码
      val unknownTopicPartitionResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.UNKNOWN_TOPIC_OR_PARTITION.code)

      /**
        * apiVersion为0表示使用旧版本请求，此时的offset存储在ZooKeeper中，所以其处理逻辑主要是ZooKeeper的读取操作。
        * 在OffsetCommitRequest的处理过程中也有根据版本号进行不同操作的相关代码，请读者注意
        * apiVersion为1表示使用新版本的请求，此时提交的offset由GroupCoordinator管理，由Kafka的主题__consumer_offsets进行管理
        */
      if (header.apiVersion == 0) { // apiVersion == 0的旧版本的处理
        // version 0 reads offsets from ZK
        // 遍历授权的主题分区
        val responseInfo = authorizedTopicPartitions.map { topicPartition =>
          // 构造构造得到封装了/consumers/[group_id]/offsets/[topic_name]和/consumers/[group_id]/owners/[topic_name]路径的ZKGroupTopicDirs对象
          val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicPartition.topic)
          try {
            if (!metadataCache.hasTopicMetadata(topicPartition.topic)) // 元数据不包含该主题
              (topicPartition, unknownTopicPartitionResponse) // 返回包含INVALID_OFFSET和UNKNOWN_TOPIC_OR_PARTITION错误码的响应
            else {
              // 从Zookeeper中读取/consumers/[group_id]/offsets/[topic_name]/[partition_id]路径的数据
              val payloadOpt = zkUtils.readDataMaybeNull(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}")._1
              payloadOpt match {
                case Some(payload) => // 能够读取到数据，根据读取的数据构造响应
                  (topicPartition, new OffsetFetchResponse.PartitionData(payload.toLong, "", Errors.NONE.code))
                case None => // 未能读取到数据，返回包含INVALID_OFFSET和UNKNOWN_TOPIC_OR_PARTITION错误码
                  (topicPartition, unknownTopicPartitionResponse)
              }
            }
          } catch {
            case e: Throwable =>
              (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "",
                Errors.forException(e).code))
          }
        }.toMap
        // 构造响应对象
        new OffsetFetchResponse((responseInfo ++ unauthorizedStatus).asJava)
      } else { // apiVersion == 1的新版本的处理，需要从Kafka的主题__consumer_offsets中读取
        // version 1 reads offsets from Kafka;
        // 使用GroupCoordinator读取offset
        val offsets = coordinator.handleFetchOffsets(offsetFetchRequest.groupId, authorizedTopicPartitions).toMap

        // Note that we do not need to filter the partitions in the
        // metadata cache as the topic partitions will be filtered
        // in coordinator's offset manager through the offset cache
        // 构造响应对象
        new OffsetFetchResponse((offsets ++ unauthorizedStatus).asJava)
      }
    }

    trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
    // 将OffsetFetchResponse放入RequestChannel中等待发送
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, offsetFetchResponse)))
  }

  // 负责处理GroupCoordinatorRequest，查询管理消费者所属的Consumer Group对应的GroupCoordinator的网络位置
  def handleGroupCoordinatorRequest(request: RequestChannel.Request) {
    // 转换请求体为GroupCoordinatorRequest对象
    val groupCoordinatorRequest = request.body.asInstanceOf[GroupCoordinatorRequest]
    // 根据correlationId构造请求头
    val responseHeader = new ResponseHeader(request.header.correlationId)

    if (!authorize(request.session, Describe, new Resource(Group, groupCoordinatorRequest.groupId))) { // 检查授权情况
      // 未授权，返回GROUP_AUTHORIZATION_FAILED错误码，Node节点被置为NO_NODE
      val responseBody = new GroupCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED.code, Node.noNode)
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // 通过groupId得到对应的Offsets Topic分区的ID
      val partition = coordinator.partitionFor(groupCoordinatorRequest.groupId)

      // get metadata (and create the topic if necessary)
      // 从MetadataCache中获取Offsets Topic的相关信息，如果Offsets Topic未创建则会创建
      val offsetsTopicMetadata = getOrCreateGroupMetadataTopic(request.securityProtocol)

      val responseBody = if (offsetsTopicMetadata.error != Errors.NONE) { // 获取出错
        // 返回GROUP_COORDINATOR_NOT_AVAILABLE错误码，Node节点被置为NO_NODE
        new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, Node.noNode)
      } else {
        // 通过Offsets Topic的分区的ID获取其Leader副本所在的节点，由该节点上的GroupCoordinator负责管理该Consumer Group组
        val coordinatorEndpoint = offsetsTopicMetadata.partitionMetadata().asScala
          .find(_.partition == partition)
          .map(_.leader())

        // 创建GroupCoordinatorResponse并返回
        coordinatorEndpoint match {
          case Some(endpoint) if !endpoint.isEmpty => // 存在，返回构造的GroupCoordinatorResponse
            new GroupCoordinatorResponse(Errors.NONE.code, endpoint)
          case _ =>
            // 不存在，返回GROUP_COORDINATOR_NOT_AVAILABLE错误码，Node节点被置为NO_NODE
            new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, Node.noNode)
        }
      }

      trace("Sending consumer metadata %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      // 将响应加入到RequestChannel中的队列中，等待发送
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request) {
    val describeRequest = request.body.asInstanceOf[DescribeGroupsRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    val groups = describeRequest.groupIds().asScala.map {
      case groupId =>
        if (!authorize(request.session, Describe, new Resource(Group, groupId))) {
          groupId -> DescribeGroupsResponse.GroupMetadata.forError(Errors.GROUP_AUTHORIZATION_FAILED)
        } else {
          val (error, summary) = coordinator.handleDescribeGroup(groupId)
          val members = summary.members.map { member =>
            val metadata = ByteBuffer.wrap(member.metadata)
            val assignment = ByteBuffer.wrap(member.assignment)
            new DescribeGroupsResponse.GroupMember(member.memberId, member.clientId, member.clientHost, metadata, assignment)
          }
          groupId -> new DescribeGroupsResponse.GroupMetadata(error.code, summary.state, summary.protocolType,
            summary.protocol, members.asJava)
        }
    }.toMap

    val responseBody = new DescribeGroupsResponse(groups.asJava)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  // 使用kafka-consumer-groups.sh脚本列出所有的Consumer Group
  def handleListGroupsRequest(request: RequestChannel.Request) {
    // 构造请求头
    val responseHeader = new ResponseHeader(request.header.correlationId)
    // 构造请求体
    val responseBody = if (!authorize(request.session, Describe, Resource.ClusterResource)) { // 未通过授权
      // 返回错误码为CLUSTER_AUTHORIZATION_FAILED的响应
      ListGroupsResponse.fromError(Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else { // 通过授权
      // 使用GroupCoordinator的handleListGroups()方法来处理
      val (error, groups) = coordinator.handleListGroups()
      // 将得到的结果构造为ListGroupsResponse响应集合
      val allGroups = groups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }
      // 构造响应对象
      new ListGroupsResponse(error.code, allGroups.asJava)
    }
    // 将响应对象放入RequestChannel等待发送
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  // 处理JoinGroupRequest请求
  def handleJoinGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    // 转换请求体为JoinGroupRequest对象
    val joinGroupRequest = request.body.asInstanceOf[JoinGroupRequest]
    // 构造响应头
    val responseHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a join-group response
    // 定义回调函数
    def sendResponseCallback(joinResult: JoinGroupResult) {
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      val responseBody = new JoinGroupResponse(joinResult.errorCode, joinResult.generationId, joinResult.subProtocol,
        joinResult.memberId, joinResult.leaderId, members)

      trace("Sending join group response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) { // 检查授权
      val responseBody = new JoinGroupResponse(
        Errors.GROUP_AUTHORIZATION_FAILED.code,
        JoinGroupResponse.UNKNOWN_GENERATION_ID, // -1
        JoinGroupResponse.UNKNOWN_PROTOCOL, // 空字符串
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId，空字符串
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId，空字符串
        Map.empty[String, ByteBuffer])
      // 将响应放入RequestChannel等待发送
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else { // 授权检查通过
      // let the coordinator to handle join-group
      // 构造可接受的PartitionAssignor协议集合
      val protocols = joinGroupRequest.groupProtocols().map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      // 将JoinGroupRequest交给GroupCoordinator的handleJoinGroup()方法处理
      coordinator.handleJoinGroup(
        joinGroupRequest.groupId, // 组ID
        joinGroupRequest.memberId, // Member ID
        request.header.clientId, // 客户端ID
        request.session.clientAddress.toString, // 客户端地址
        joinGroupRequest.sessionTimeout, // 客户端配置的超时时间
        joinGroupRequest.protocolType,
        protocols, // 客户端可接受的PartitionAssignor协议
        sendResponseCallback) // 回调
    }
  }

  // 处理SyncGroupRequest请求
  def handleSyncGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    // 转换请求体为SyncGroupRequest对象
    val syncGroupRequest = request.body.asInstanceOf[SyncGroupRequest]

    // 回调函数
    def sendResponseCallback(memberState: Array[Byte], errorCode: Short) {
      val responseBody = new SyncGroupResponse(errorCode, ByteBuffer.wrap(memberState))
      val responseHeader = new ResponseHeader(request.header.correlationId)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) { // 检查授权
      // 授权未通过，直接调用回调函数返回GROUP_AUTHORIZATION_FAILED错误码
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED.code)
    } else {
      // 授权通过，交给GroupCoordinator的handleSyncGroup()方法处理
      coordinator.handleSyncGroup(
        syncGroupRequest.groupId(), // Group ID
        syncGroupRequest.generationId(), // Group的年代信息
        syncGroupRequest.memberId(), // Member ID
        syncGroupRequest.groupAssignment().mapValues(Utils.toArray(_)), // 分配结果
        sendResponseCallback
      )
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    // 转换请求体对象为HeartbeatRequest类型
    val heartbeatRequest = request.body.asInstanceOf[HeartbeatRequest]
    // 构造响应头
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a heartbeat response
    // 回调函数
    def sendResponseCallback(errorCode: Short) {
      val response = new HeartbeatResponse(errorCode)
      trace("Sending heartbeat response %s for correlation id %d to client %s."
        .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    // 检查授权
    if (!authorize(request.session, Read, new Resource(Group, heartbeatRequest.groupId))) {
      // 授权未通过
      val heartbeatResponse = new HeartbeatResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, heartbeatResponse)))
    }
    else { // 授权通过
      // let the coordinator to handle heartbeat
      // 交由GroupCoordinator的handleHeartbeat()方法处理
      coordinator.handleHeartbeat(
        heartbeatRequest.groupId(), // Group ID
        heartbeatRequest.memberId(), // Member ID
        heartbeatRequest.groupGenerationId(), // Group年代信息
        sendResponseCallback)
    }
  }

  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  private def instantiateQuotaManagers(cfg: KafkaConfig): Map[Short, ClientQuotaManager] = {
    val producerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val consumerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val quotaManagers = Map[Short, ClientQuotaManager](
      ApiKeys.PRODUCE.id ->
              new ClientQuotaManager(producerQuotaManagerCfg, metrics, ApiKeys.PRODUCE.name, new org.apache.kafka.common.utils.SystemTime),
      ApiKeys.FETCH.id ->
              new ClientQuotaManager(consumerQuotaManagerCfg, metrics, ApiKeys.FETCH.name, new org.apache.kafka.common.utils.SystemTime)
    )
    quotaManagers
  }

  // 处理LeaveGroupRequest请求
  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    // 转换请求体为LeaveGroupRequest对象
    val leaveGroupRequest = request.body.asInstanceOf[LeaveGroupRequest]
    // 构造响应头
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a leave-group response
    // 定义回调函数
    def sendResponseCallback(errorCode: Short) {
      val response = new LeaveGroupResponse(errorCode)
      trace("Sending leave group response %s for correlation id %d to client %s."
                    .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    if (!authorize(request.session, Read, new Resource(Group, leaveGroupRequest.groupId))) { // 检查授权
      // 授权未通过
      val leaveGroupResponse = new LeaveGroupResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, leaveGroupResponse)))
    } else {
      // let the coordinator to handle leave-group
      // 授权通过，交给GroupCoordinator的handleLeaveGroup()处理
      coordinator.handleLeaveGroup(
        leaveGroupRequest.groupId(),
        leaveGroupRequest.memberId(),
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request) {
    val respHeader = new ResponseHeader(request.header.correlationId)
    val response = new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE.code, config.saslEnabledMechanisms)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on a SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
      ApiVersionsResponse.apiVersionsResponse
    else
      ApiVersionsResponse.fromError(Errors.UNSUPPORTED_VERSION)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def close() {
    quotaManagers.foreach { case (apiKey, quotaManager) =>
      quotaManager.shutdown()
    }
    info("Shutdown complete.")
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, ClusterAction, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }
}
