/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    // 心跳任务辅助类
    private final Heartbeat heartbeat;
    // 定时任务，用于定时发送心跳请求和对响应的处理，会被添加到ConsumerNetworkClient的delayedTasks队列中
    private final HeartbeatTask heartbeatTask;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    // 当前消费者所属的Consumer Group的id
    protected final String groupId;
    // 负责网络通信和执行定时任务
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;

    // 是否需要执行发送JoinGroupRequest请求前的准备操作
    private boolean needsJoinPrepare = true;
    // 是否重新发送JoinGroupRequest请求的条件之一
    private boolean rejoinNeeded = true;
    // 记录服务端GroupCoordinator所在的Node节点
    protected Node coordinator;
    // 服务端GroupCoordinator返回的分配给消费者的唯一ID
    protected String memberId;
    protected String protocol;
    /**
     * 服务端GroupCoordinator返回的年代信息，用来区分两次Rebalance操作
     * 由于网络延迟等问题，在执行Rebalance操作时可能收到上次Rebalance过程的请求，
     * 为了避免这种干扰，每次Rebalance操作都会递增generation值
     */
    protected int generation;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        // 年代信息
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID; // -1
        // Consumer Member ID
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID; // ""
        this.groupId = groupId;
        this.coordinator = null;
        this.sessionTimeoutMs = sessionTimeoutMs;
        // 心跳操作辅助对象
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        // 心跳任务
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of protocols implements (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     * 该方法会阻塞，直到GroupCoordinator已就绪且可以接受请求
     * 如果GroupCoordinator不正常，会发送GroupCoordinatorRequest请求
     */
    public void ensureCoordinatorReady() {
        // 检测是否需要重新查找GroupCoordinator
        while (coordinatorUnknown()) {
            // 需要查找GroupCoordinator
            // 查找负载最低的Node节点，创建GroupCoordinatorRequest请求
            RequestFuture<Void> future = sendGroupCoordinatorRequest();
            // 发送GroupCoordinatorRequest请求，该方法会阻塞，直到接收到GroupCoordinatorResponse响应
            client.poll(future);

            // 检测future的状态，查看是否有异常
            if (future.failed()) {
                // 出现异常
                if (future.isRetriable())
                    // 异常是RetriableException，则阻塞更新Metadata元数据
                    client.awaitMetadataUpdate();
                else
                    // 异常不是RetriableException，抛出
                    throw future.exception();
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                // 连接不到GroupCoordinator，退避一段时间后重试
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }

        }
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected boolean needRejoin() {
        return rejoinNeeded;
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        /**
         * 检测是否使用了AUTO_TOPICS或AUTO_PATTERN模式，检测rejoinNeeded和needsPartitionAssignment两个字段的值
         * 该方法由子类 {@link ConsumerCoordinator#needRejoin} 重写了
         * 如果不需要重新发送JoinGroupRequest，就直接返回
         */
        if (!needRejoin())
            return;

        if (needsJoinPrepare) {
            onJoinPrepare(generation, memberId);
            needsJoinPrepare = false;
        }

        while (needRejoin()) {
            // 检测GroupCoordinator是否就绪
            ensureCoordinatorReady();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            // 查看是否还有发往GroupCoordinator所在Node的请求
            if (client.pendingRequestCount(this.coordinator) > 0) {
                // 等待正在发送的请求发送完成并收到响应，避免重复发送JoinGroupRequest
                client.awaitPendingRequests(this.coordinator); // 会阻塞
                continue;
            }

            // 创建JoinGroupRequest
            RequestFuture<ByteBuffer> future = sendJoinGroupRequest();
            // 添加RequestFutureListener
            future.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    onJoinComplete(generation, memberId, protocol, value);
                    needsJoinPrepare = true;
                    // 重启心跳定时任务
                    heartbeatTask.reset();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                }
            });
            
            // 使用ConsumerNetworkClient发送JoinGroupRequest，会阻塞直到收到JoinGroupResponse或出现异常
            client.poll(future);

            // 检测发送是否失败
            if (future.failed()) {
                // 出现异常
                RuntimeException exception = future.exception();
                // 当异常是未知Consumer、正在重均衡、GroupCoordinator版本对不上时，直接尝试新的请求
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException)
                    continue;
                // 是否可重试
                else if (!future.isRetriable())
                    // 不可重试，抛出异常
                    throw exception;
                // 可以重试，等待退避时间后再次重试
                time.sleep(retryBackoffMs);
            }
        }
    }

    // 定时任务，负责定时发送HeartBeatRequest并处理其响应
    private class HeartbeatTask implements DelayedTask {

        private boolean requestInFlight = false;

        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            // 获取当前时间
            long now = time.milliseconds();
            // 重置最后一次心跳时间lastSessionReset为当前时间
            heartbeat.resetSessionTimeout(now);
            // 将当前HeartbeatTask任务对象从delayedTasks队列中移除
            client.unschedule(this);

            // 没有正在发送的心跳请求时
            if (!requestInFlight)
                // 使用ConsumerNetworkClient重新调度心跳任务
                client.schedule(this, now);
        }

        @Override
        public void run(final long now) {
			/**
			 * 检查是否需要发送HeartbeatRequest
			 * 1. GroupCoordinator已确定且已连接
			 * 2. 不处于正在等待Partition分配结果的状态
			 * 3. 之前的HeartbeatRequest请求正常收到响应且没有过期
			 */
			if (generation < 0 || needRejoin() || coordinatorUnknown()) {
                // no need to send the heartbeat we're not using auto-assignment or if we are
                // awaiting a rebalance
                return;
            }

            // 检测HeartbeatResponse是否超时，若超时则认为GroupCoordinator宕机
            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
				// 清空unsent集合中该GroupCoordinator所在Node对应的请求队列并将这些请求标记为异常
                coordinatorDead();
                return;
            }

            // 检测HeartbeatTask是否到期
            if (!heartbeat.shouldHeartbeat(now)) {
				/**
				 * 如果未到期，更新到期时间，将HeartbeatTask对象重新添加到DelayedTaskQueue中
				 * 注意，此时的时间已经更新为now + heartbeat.timeToNextHeartbeat(now)
				 */
                // we don't need to heartbeat now, so reschedule for when we do
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {
				// 已到期，更新最近发送HeartbeatRequest请求的时间，即将lastHeartbeatSend更新为当前时间
                heartbeat.sentHeartbeat(now);
                // 更新该字段，表示有HeartbeatRequest请求正在发送，还未收到响应，防止重复发送
                requestInFlight = true;

                // 发送心跳请求
                RequestFuture<Void> future = sendHeartbeatRequest();
                // 在返回的RequestFuture上添加RequestFutureListener监听器
                future.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        // 心跳响应成功处理
                        requestInFlight = false;
                        long now = time.milliseconds();
                        // 更新最后一次收到心跳响应的时间
                        heartbeat.receiveHeartbeat(now);
                        // 计算下一次执行心跳任务的时间
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        // 根据下一次执行心跳任务的时间重新添加心跳任务
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        // 心跳响应处理失败
                        requestInFlight = false;
                        // 重新规划心跳任务，执行时间为等待退避时间段之后
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     * @return A request future which wraps the assignment returned from the group leader
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        // 检查GroupCoordinator是否就绪
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group {}", groupId);
        // 构造JoinGroupRequest对象
        JoinGroupRequest request = new JoinGroupRequest(
                groupId,
                this.sessionTimeoutMs,
                this.memberId,
                protocolType(),
                metadata());

        log.debug("Sending JoinGroup ({}) to coordinator {}", request, this.coordinator);
        // 使用ConsumerNetworkClient将请求暂存入unsent，等待发送
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                // 适配响应处理器
                .compose(new JoinGroupResponseHandler());
    }


    // 处理JoinGroupResponse
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {

        @Override
        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }
    
        // 处理JoinGroupResponse的流程入口
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            // 获取错误并进行错误处理
            Errors error = Errors.forCode(joinResponse.errorCode());
            if (error == Errors.NONE) {
                // 无错误
                log.debug("Received successful join group response for group {}: {}", groupId, joinResponse.toStruct());
                AbstractCoordinator.this.memberId = joinResponse.memberId();
                AbstractCoordinator.this.generation = joinResponse.generationId();
                AbstractCoordinator.this.rejoinNeeded = false;
                AbstractCoordinator.this.protocol = joinResponse.groupProtocol();
                sensors.joinLatency.record(response.requestLatencyMs());
                // 是否是Leader
                if (joinResponse.isLeader()) {
                    // 如果是Leader，在该方法中会进行分区分配，并将分配结果反馈给服务端
                    onJoinLeader(joinResponse).chain(future);
                } else {
                    // 如果是Follower，也会发送进行同步分区分配的请求
                    onJoinFollower().chain(future);
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                        coordinator);
                // backoff and retry
                // 抛出异常，GroupCoordinator正在加载组
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                // 抛出异常，未知Consumer ID
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                // GroupCoordinator不可用，没有对应组的GroupCoordinator
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                // 抛出异常
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // 协议版本对不上，可能是由于消费者和服务端使用的Kafka版本不同
                // 会话过期
                // 无效的组ID
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                // 抛出异常
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 抛出异常，组授权失败
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                // 其他异常
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        // 创建follower的同步分区信息，参数groupAssignment空字典
        SyncGroupRequest request = new SyncGroupRequest(groupId, generation,
                memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
        // 发送同步分区的请求
        return sendSyncGroupRequest(request);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // 进行分区分配，最终刚返回的分配结果，其中键是ConsumerID，值是序列化后的Assignment对象
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());

            // 将分组信息包装为SyncGroupRequest
            SyncGroupRequest request = new SyncGroupRequest(groupId, generation, memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
            // 发送分组信息，并返回
            return sendSyncGroupRequest(request);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        // 检查GroupCoordinator是否就绪
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        // 将发送分区分配信息的请求暂存到unsent集合，
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                .compose(new SyncGroupResponseHandler());
    }

    // 处理SyncGroupResponse响应的方法
    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {

        @Override
        public SyncGroupResponse parse(ClientResponse response) {
            return new SyncGroupResponse(response.responseBody());
        }

        // 处理入口
        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            // 获取异常
            Errors error = Errors.forCode(syncResponse.errorCode());
            if (error == Errors.NONE) {
                // 无异常
                log.info("Successfully joined group {} with generation {}", groupId, generation);
                sensors.syncLatency.record(response.requestLatencyMs());
                // 调用RequestFuture.complete()方法传播分区分配结果
                future.complete(syncResponse.memberAssignment());
            } else {
                // 有异常
                // 将rejoinNeeded置为true
                AbstractCoordinator.this.rejoinNeeded = true;
                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    // 组授权失败
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    // 正在Rebalance过程中
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    // 无效的Member ID，或无效的年代信息
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    // GroupCoordinator不可以用，没有Group对应的Coordinator
                    coordinatorDead();
                    future.raise(error);
                } else {
                    // 其他异常
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest() {
        // initiate the group metadata request
        // find a node to ask about the coordinator
        // 查找负载最低的Node节点
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            // 无节点可用，返回包含NoAvailableBrokersException的RequestFuture
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a group  metadata request
            log.debug("Sending coordinator request for group {} to broker {}", groupId, node);
            // 创建GroupCoordinatorRequest请求
            GroupCoordinatorRequest metadataRequest = new GroupCoordinatorRequest(this.groupId);
            // 使用ConsumerNetworkClient发送请求，返回经过compose()适配的RequestFuture<Void>对象
            return client.send(node, ApiKeys.GROUP_COORDINATOR, metadataRequest)
                    .compose(new RequestFutureAdapter<ClientResponse, Void>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                            // 处理GroupMetadataResponse的入口方法
                            handleGroupMetadataResponse(response, future);
                        }
                    });
        }
    }

    // 处理GroupMetadataResponse的入口方法
    private void handleGroupMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Received group coordinator response {}", resp);
    
        /**
         * 检测是否已经找到GroupCoordinator且成功连接
         * 这是由于在发送GroupCoordinatorRequest的时候并没有防止重发
         * 因此可能会有多个GroupCoordinatorResponse
         */
        if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            // 如果是，则忽略该GroupCoordinatorResponse
            future.complete(null);
        } else {
            // 将响应体封装为GroupCoordinatorResponse对象
            GroupCoordinatorResponse groupCoordinatorResponse = new GroupCoordinatorResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            // 获取响应中的异常信息
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            if (error == Errors.NONE) {
                // 无异常，根据响应信息构建一个Node节点对象赋值给coordinator
                this.coordinator = new Node(Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                        groupCoordinatorResponse.node().host(),
                        groupCoordinatorResponse.node().port());

                log.info("Discovered coordinator {} for group {}.", coordinator, groupId);

                // 尝试与coordinator建立连接
                client.tryConnect(coordinator);

                // start sending heartbeats only if we have a valid generation
                // 启动定时心跳任务
                if (generation > 0)
                    heartbeatTask.reset();
                // 将正常收到的GroupCoordinatorResponse事件传播出去
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // GroupCoordinator未授权异常，传播异常
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // 传播其他异常
                future.raise(error);
            }
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     * 检测是否需要重新查找GroupCoordinator
     */
    public boolean coordinatorUnknown() {
        // coordinator是否为null
        if (coordinator == null)
            return true;

        // 检测与GroupCoordinator的连接是否正常
        if (client.connectionFailed(coordinator)) {
            // 如果不正常，标记GroupCoordinator已死
            coordinatorDead();
            return true;
        }

        return false;
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            // 将GroupCoordinator设置为null，表示需要重选GroupCoordinator
            this.coordinator = null;
        }
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        maybeLeaveGroup();
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public void maybeLeaveGroup() {
        client.unschedule(heartbeatTask);
        if (!coordinatorUnknown() && generation > 0) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            sendLeaveGroupRequest();
        }

        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        rejoinNeeded = true;
    }

    private void sendLeaveGroupRequest() {
        LeaveGroupRequest request = new LeaveGroupRequest(groupId, memberId);
        RequestFuture<Void> future = client.send(coordinator, ApiKeys.LEAVE_GROUP, request)
                .compose(new LeaveGroupResponseHandler());

        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {}

            @Override
            public void onFailure(RuntimeException e) {
                log.debug("LeaveGroup request for group {} failed with error", groupId, e);
            }
        });

        client.poll(future, 0);
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public LeaveGroupResponse parse(ClientResponse response) {
            return new LeaveGroupResponse(response.responseBody());
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            // process the response
            short errorCode = leaveResponse.errorCode();
            if (errorCode == Errors.NONE.code())
                future.complete(null);
            else
                future.raise(Errors.forCode(errorCode));
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    public RequestFuture<Void> sendHeartbeatRequest() {
    	// 创建心跳对象
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);
        // 使用ConsumerNetworkClient发送心跳，此时会将请求暂存到unsent集合，等待ConsumerNetworkClient的poll发送
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
				// 同时使用HeartbeatCompletionHandler将RequestFuture<ClientResponse>适配成RequestFuture<Void>
                .compose(new HeartbeatCompletionHandler());
    }

    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
		/**
		 * 具体业务线由父类CoordinatorResponseHandler处理
		 * 父类会调用该方法解析ClientResponse
		 * 而HeartbeatCompletionHandler子类实现中才真正对ClientResponse进行解析
		 * 最终解析为一个HeartbeatResponse对象
		 * @param response 未解析的响应
		 * @return 解析后的得到的响应
		 */
        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }
	
		/**
		 * 具体业务线由父类CoordinatorResponseHandler处理
		 * 父类会调用该方法解析ClientResponse
		 * 而HeartbeatCompletionHandler子类实现中才真正对ClientResponse进行解析
		 * 最终解析为一个HeartbeatResponse对象
		 * @param heartbeatResponse 已解析的响应
		 * @param future
		 */
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            // 获取响应错误
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            if (error == Errors.NONE) {
            	// 没有响应错误
                log.debug("Received successful heartbeat response for group {}", groupId);
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
            	// GroupCoordinator不可用，或没有Group对应的Coordinator
                log.debug("Attempt to heart beat failed for group {} since coordinator {} is either not started or not valid.",
                        groupId, coordinator);
                // 表示GroupCoordinator已失效
                coordinatorDead();
                // 抛出异常
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
            	// 正在Rebalance过程中
                log.debug("Attempt to heart beat failed for group {} since it is rebalancing.", groupId);
				// 标记rejoinNeeded为true，重新发送JoinGroupRequest请求尝试重新加入Consumer Group
                AbstractCoordinator.this.rejoinNeeded = true;
				// 抛出异常
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {
            	// 表示HeartbeatRequest携带的generationId过期，说明可能进行了一次Rebalance操作
                log.debug("Attempt to heart beat failed for group {} since generation id is not legal.", groupId);
                // 标记rejoinNeeded为true，重新发送JoinGroupRequest请求尝试重新加入Consumer Group
                AbstractCoordinator.this.rejoinNeeded = true;
				// 抛出异常
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
            	// GroupCoordinator无法识别该Consumer
                log.debug("Attempt to heart beat failed for group {} since member id is not valid.", groupId);
                // 清空memberId
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
				// 标记rejoinNeeded为true，重新发送JoinGroupRequest请求尝试重新加入Consumer Group
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            	// GroupCoordinator授权失败
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T>
            extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        // 对ClientResponse进行解析
        public abstract R parse(ClientResponse response);

        // 对解析后的响应进行处理
        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException)
                coordinatorDead();
            future.raise(e);
        }

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                // 解析响应
                R responseObj = parse(clientResponse);
                // 处理响应
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat"),
                lastHeartbeat);
        }
    }

}
