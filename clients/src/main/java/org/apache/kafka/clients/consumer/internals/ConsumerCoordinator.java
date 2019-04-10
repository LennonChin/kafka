/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.TopicConstants;
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
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);
    
    /**
     * 在消费者发送的JoinGroupRequest请求中包含了消费者自身支持的PartitionAssignor信息，
     * GroupCoordinator从所有消费者都支持的分配策略中选择一个，通知Leader使用此分配策略进行分区分配。
     * 此字段的值通过partition.assignment.strategy参数配置，可以配置多个。
     */
    private final List<PartitionAssignor> assignors;
    // Kafka集群元数据
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    // 是否开启了自动提交offset（enable.auto.commit）
    private final boolean autoCommitEnabled;
    // 自动提交offset的定时任务
    private final AutoCommitTask autoCommitTask;
    // 拦截器集合
    private final ConsumerInterceptors<?, ?> interceptors;
    // 是否排除内部Topic
    private final boolean excludeInternalTopics;
    /**
     * 用来存储Metadata的快照信息，主要用来检测Topic是否发生了分区数量的变化。
     * 在ConsumerCoordinator的构造方法中，会为Metadata添加一个监听器，当Metadata更新时会做下面几件事：
     * - 如果是AUTO_PATTERN模式，则使用用户自定义的正则表达式过滤Topic，
     *      得到需要订阅的Topic集合后，设置到SubscriptionState的subscription集合和groupSubscription集合中。
     * - 如果是AUTO_PATTERN或AUTO_TOPICS模式，为当前Metadata做一个快照，这个快照底层是使用HashMap记录每个Topic中Partition的个数。
     *      将新旧快照进行比较，发生变化的话，则表示消费者订阅的Topic发生分区数量变化，
     *      则将SubscriptionState的needsPartitionAssignment字段置为true，需要重新进行分区分配。
     * - 使用metadataSnapshot字段记录变化后的新快照。
     */
    private MetadataSnapshot metadataSnapshot;
    /**
     * 用来存储Metadata的快照信息，不过是用来检测Partition分配的过程中有没有发生分区数量变化。
     * 具体是在Leader消费者开始分区分配操作前，使用此字段记录Metadata快照；
     * 收到SyncGroupResponse后，会比较此字段记录的快照与当前Metadata是否发生变化。
     * 如果发生变化，则要重新进行分区分配。在后面的介绍中还会分析上述过程。
     */
    private MetadataSnapshot assignmentSnapshot;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               OffsetCommitCallback defaultOffsetCommitCallback,
                               boolean autoCommitEnabled,
                               long autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics) {
        // 调用父类AbstractCoordinator的构造器
        super(client,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        this.metadata = metadata;

        // 设置强制更新集群元数据
        this.metadata.requestUpdate();
        // 根据消费者的SubscriptionState实例和集群元数据构建元数据快照
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        // 记录消费者的SubscriptionState实例
        this.subscriptions = subscriptions;
        // offset提交回调
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        // 是否自动提交offset
        this.autoCommitEnabled = autoCommitEnabled;
        // 分区分配器集合
        this.assignors = assignors;

        // 添加Metadata监听器
        addMetadataListener();

        if (autoCommitEnabled) {
            // 如果设置了自动提交offset，根据配置提交间隔时间，创建AutoCommitTask任务
            this.autoCommitTask = new AutoCommitTask(autoCommitIntervalMs);
            // 启动AutoCommitTask
            this.autoCommitTask.reschedule();
        } else {
            this.autoCommitTask = null;
        }

        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        // 拦截器集合
        this.interceptors = interceptors;
        // 是否排除Kafka内部使用的Topic
        this.excludeInternalTopics = excludeInternalTopics;
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(subscriptions.subscription());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                // AUTO_PATTERN模式的处理
                if (subscriptions.hasPatternSubscription()) {
                    // 权限验证
                    Set<String> unauthorizedTopics = new HashSet<String>();
                    for (String topic : cluster.unauthorizedTopics()) {
                        if (filterTopic(topic))
                            unauthorizedTopics.add(topic);
                    }
                    if (!unauthorizedTopics.isEmpty())
                        throw new TopicAuthorizationException(unauthorizedTopics);

                    // 定义一个List装载需要订阅的主题
                    final List<String> topicsToSubscribe = new ArrayList<>();
                    // 遍历Cluster存储的集群元数据中的Topic信息
                    for (String topic : cluster.topics())
                        // 过滤Topic将匹配的Topic加入到topicsToSubscribe
                        if (filterTopic(topic))
                            topicsToSubscribe.add(topic);

                    // 更新subscriptions集合、groupSubscription集合、assignment集合
                    subscriptions.changeSubscription(topicsToSubscribe);
                    // 更新Metadata需要记录元数据的Topic集合
                    metadata.setTopics(subscriptions.groupSubscription());
                } else if (!cluster.unauthorizedTopics().isEmpty()) {
                    // 当非AUTO_PATTERN模式时，如果非授权的主题不为空，则抛出异常
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                }

                // check if there are any changes to the metadata which should trigger a rebalance
                // 检测是否为AUTO_PATTERN或AUTO_TOPICS模式
                if (subscriptions.partitionsAutoAssigned()) {
                    // 根据新的subscriptions和cluster数据创建快照
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    // 比较新旧快照，如果不相等，则更新快照，并标识需要重新进行分区分配
                    if (!snapshot.equals(metadataSnapshot)) {
                        // 记录快照
                        metadataSnapshot = snapshot;
                        // 更新needsPartitionAssignment字段为true，表示需要重新进行分区分配
                        subscriptions.needReassignment();
                    }
                }

            }
        });
    }

    private boolean filterTopic(String topic) {
        return subscriptions.getSubscribedPattern().matcher(topic).matches() &&
                !(excludeInternalTopics && TopicConstants.INTERNAL_TOPICS.contains(topic));
    }

    private PartitionAssignor lookupAssignor(String name) {
    	// 遍历所有的PartitionAssignor
        for (PartitionAssignor assignor : this.assignors) {
        	// 匹配对应的PartitionAssignor：range或roundrobin
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    // 处理从SyncGroupResponse中的到的分区分配结果
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        // if we were the assignor, then we need to make sure that there have been no metadata updates
        // since the rebalance begin. Otherwise, we won't rebalance again until the next metadata change
		// 对比记录的Metadata快照和最新的Metadata快照，如果不一致则说明分配过程中出现了Topic增删或分区数量的变化
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot)) {
			// 去除groupSubscription中除subscription集合所有元素之外的元素，将needsPartitionAssignment置为true
            subscriptions.needReassignment();
            return;
        }

        // 获取指定的分区器
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // 获取分区分配信息
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
		// 设置needsFetchCommittedOffsets为true
        subscriptions.needRefreshCommits();

        // update partition assignment
		// 根据新的分区信息更新SubscriptionState的assignment集合
        subscriptions.assignFromSubscribed(assignment.partitions());

        // give the assignor a chance to update internal state based on the received assignment
		// 将assignment传递给onAssignment()方法，让分区分配器有机会更新内部状态
		// 该方法默认是空实现，用户自定义分区器时可以重写该方法
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
		// 如果是自动提交offset，重新规划自动提交周期
        if (autoCommitEnabled)
            autoCommitTask.reschedule();

        // execute the user's callback after rebalance
		// 获取Rebalance监听器
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            // 调用监听器
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                    listener.getClass().getName(), groupId, e);
        }
    }
	
	/**
	 *
	 * @param leaderId The id of the leader (which is this member)
	 * @param assignmentStrategy 分配策略，这个参数传入的是joinResponse.groupProtocol()
	 * @param allSubscriptions 组内所有的消费者的信息
	 * @return
	 */
    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
    	// 根据group_protocol字段指定分区的分配策略，查找对应的PartitionAssignor对象
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
        	// PartitionAssignor为空将会抛出IllegalStateException异常
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();
        /**
         * allSubscriptions是GroupCoordinator返回的Group组内的Member信息，遍历该字典，进行数据整理
         * 键是Member ID，值是序列化后的Subscription对象，其中记录了该Member订阅的主题等信息
         */
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
        	// 反序列化值，反序列化为Subscription对象，保存了MemberID对应的消费者所订阅的Topic信息
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            // 添加到subscriptions字典中进行记录
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            // 将该Member订阅的主题添加到allSubscribedTopics集合进行保存
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
		/**
		 * 注意，由于本方法是由父类的onJoinLeader()方法内调用的，
		 * 所以此时的this对象即是leader的ConsumerCoordinator对象
		 * 此处是让leader记录所有的订阅主题信息
		 */
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        // 更新元数据中的主题信息
        metadata.setTopics(this.subscriptions.groupSubscription());

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
		// 更新集群元数据信息
        client.ensureFreshMetadata();
        // 存储Metadata快照（通过Metadata的Listener创建的）
        assignmentSnapshot = metadataSnapshot;

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}",
                groupId, assignor.name(), subscriptions);

        // 进行分区分配
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        log.debug("Finished assignment for group {}: {}", groupId, assignment);

        // 将分区分配的结果进行序列化存入groupAssignment字典
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        // 返回序列化后的分区分配结果
        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
		// 如果设置了自动提交offset，则进行一次同步提交offset操作
        maybeAutoCommitOffsetsSync();

        // execute the user's callback before rebalance
		// 调用SubscriptionState中设置的ConsumerRebalanceListener
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            // 调用ConsumerRebalanceListener的回调方法，告诉监听者当前的分区分配方案已废除
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                    listener.getClass().getName(), groupId, e);
        }

        assignmentSnapshot = null;
        // 将needsPartitionAssignment设置为true
        subscriptions.needReassignment();
    }

    @Override
    public boolean needRejoin() {
        return subscriptions.partitionsAutoAssigned() && // 检测subscriptionType
                (super.needRejoin() || // 检测rejoinNeeded值
						subscriptions.partitionAssignmentNeeded() // 检测needsPartitionAssignment
				);
    }

    /**
     * Refresh the committed offsets for provided partitions.
	 * 发送OffsetFetchRequest请求，从服务端拉取最近提交的offset集合，并更新到subscriptions集合
     */
    public void refreshCommittedOffsetsIfNeeded() {
    	// 根据needsFetchCommittedOffsets字段判断是否需要更新
        if (subscriptions.refreshCommitsNeeded()) {
        	// 发送OffsetFetchRequest请求，并处理响应数据，得到新的offset信息
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            // 遍历得到的offset信息并进行更新
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
				// 判断对应的TopicPartition在subscriptions是否有记录
                if (subscriptions.isAssigned(tp))
                	// 如果有则更新偏移量
                    this.subscriptions.committed(tp, entry.getValue());
            }
            // 更新完毕，将needsFetchCommittedOffsets置为false
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
	 * 发送OffsetFetchRequest并处理响应
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
        	// 保证GroupCoordinator是就绪的
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
			// 构造OffsetFetchRequest，暂存到unsent中，等待发送
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            // 发送请求，会阻塞
            client.poll(future);
            
            // 判断是否发送成功
            if (future.succeeded())
            	// 返回从服务端得到的offset
                return future.value();

            // 如果失败，判断是否可以重试
            if (!future.isRetriable())
            	// 不可重试，抛出异常
                throw future.exception();

            // 可以重试，退避一段时间后再重试
            time.sleep(retryBackoffMs);
        }
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     */
    public void ensurePartitionAssignment() {
    	// 订阅模式是否是AUTO_TOPICS或AUTO_PATTERN
        if (subscriptions.partitionsAutoAssigned()) {
            // Due to a race condition between the initial metadata fetch and the initial rebalance, we need to ensure that
            // the metadata is fresh before joining initially, and then request the metadata update. If metadata update arrives
            // while the rebalance is still pending (for example, when the join group is still inflight), then we will lose
            // track of the fact that we need to rebalance again to reflect the change to the topic subscription. Without
            // ensuring that the metadata is fresh, any metadata update that changes the topic subscriptions and arrives with a
            // rebalance in progress will essentially be ignored. See KAFKA-3949 for the complete description of the problem.
			// 是否是AUTO_PATTERN
            if (subscriptions.hasPatternSubscription())
            	// 如有需要，更新元数据
                client.ensureFreshMetadata();

            // 主要的流程代码
            ensureActiveGroup();
        }
    }

    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync();
        } finally {
            super.close();
        }
    }


    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    	// 将needsFetchCommittedOffset设置为true
        this.subscriptions.needRefreshCommits();
		// 创建并缓存OffsetCommitRequest请求，等待发送，响应由OffsetCommitResponseHandler处理
		RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
		// 选择回调函数
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        // 添加监听器
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                // 调用回调
                cb.onComplete(offsets, null);
            }

            @Override
            public void onFailure(RuntimeException e) {
            	// 异常处理
                if (e instanceof RetriableException) {
                    cb.onComplete(offsets, new RetriableCommitFailedException("Commit offsets failed with retriable exception. You should retry committing offsets.", e));
                } else {
                    cb.onComplete(offsets, e);
                }
            }
        });

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
		// 发送OffsetCommitRequest
        client.pollNoWakeup();
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     */
    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    	// 检查合法性
        if (offsets.isEmpty())
            return;

        while (true) {
        	// 检查GroupCoordinator状态是否就绪
            ensureCoordinatorReady();

            // 发送offset提交请求
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future);

            // 判断发送结果是否成功
            if (future.succeeded()) {
            	// 拦截器处理
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return;
            }

            // 走到这里说明发送失败了，判断是否可以重试
            if (!future.isRetriable())
            	// 不能重试，直接抛出异常
                throw future.exception();

            // 可以重试，退避一段时间再重试
            time.sleep(retryBackoffMs);
        }
    }

    // 定时任务，用于周期性地调用commitOffsetAsync()方法自动提交offset
    private class AutoCommitTask implements DelayedTask {
        // 提交间隔时间
        private final long interval;

        public AutoCommitTask(long interval) {
            this.interval = interval;
        }

        private void reschedule() {
            client.schedule(this, time.milliseconds() + interval);
        }

        private void reschedule(long at) {
            client.schedule(this, at);
        }

        public void run(final long now) {
            if (coordinatorUnknown()) {
                log.debug("Cannot auto-commit offsets for group {} since the coordinator is unknown", groupId);
                reschedule(now + retryBackoffMs);
                return;
            }

            if (needRejoin()) {
                // skip the commit when we're rejoining since we'll commit offsets synchronously
                // before the revocation callback is invoked
                reschedule(now + interval);
                return;
            }

            // 异步提交偏移量
            commitOffsetsAsync(subscriptions.allConsumed(), new OffsetCommitCallback() {
            	// 在提交成功的回调中重新规划下一次提交计划
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception == null) {
                        reschedule(now + interval);
                    } else {
                        log.warn("Auto offset commit failed for group {}: {}", groupId, exception.getMessage());
                        reschedule(now + interval);
                    }
                }
            });
        }
    }

    private void maybeAutoCommitOffsetsSync() {
    	// 是否是同步提交offset
        if (autoCommitEnabled) {
            try {
            	// 提交offset
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (WakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto offset commit failed for group {}: {}", groupId, e.getMessage());
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    	// 检查GroupCoordinator是否就绪
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // 检查offsets参数是否为空，如果为空则不提交
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // create the offset commit request
		// 转换offsets参数，转换格式如下
		// Map<TopicPartition, OffsetAndMetadata> -> Map<TopicPartition, OffsetCommitRequest.PartitionData>
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        // 根据转换得到的offsetData构造OffsetCommitRequest请求对象
        OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.memberId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

        log.trace("Sending offset-commit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);

        // 使用ConsumerNetworkClient暂存请求到unsent，等待发送
        return client.send(coordinator, ApiKeys.OFFSET_COMMIT, req)
				// 适配响应处理
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    public static class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit failed.", exception);
        }
    }

    // 处理OffsetCommitResponse响应的方法
    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        // 处理入口
        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            // 遍历获得的OffsetCommitResponse中responseData字典
            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
            	// 获取TopicPartition
                TopicPartition tp = entry.getKey();
                // 根据TopicPartition获取对应的当时提交的OffsetAndMetadata
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                // 获取对应的当时提交的offset
                long offset = offsetAndMetadata.offset();

                // 获取错误
                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                	// 无异常
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    // 判断subscriptions的assignment中是否记录了响应的TopicPartition
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                    	// 如果记录了就更新相应的偏移量信息
                        subscriptions.committed(tp, offsetAndMetadata);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    // GroupCoordinator未授权异常
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                	// Topic未授权异常
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                	// 提交的偏移量信息过大异常
					// 无效的偏移量大小异常
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                	// Group正在加载中
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                	// GroupCoordinator不可用
					// 对于相应的Group没有对应的Coordinator
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                	// 未知的MemberID
					// 未知的年代信息
					// 正在Rebalance过程中
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    subscriptions.needReassignment();
                    future.raise(new CommitFailedException("Commit cannot be completed since the group has already " +
                            "rebalanced and assigned the partitions to another member. This means that the time " +
                            "between subsequent calls to poll() was longer than the configured session.timeout.ms, " +
                            "which typically implies that the poll loop is spending too much time message processing. " +
                            "You can address this either by increasing the session timeout or by reducing the maximum " +
                            "size of batches returned in poll() with max.poll.records."));
                    return;
                } else {
                	// 其他异常
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
    	// 检查GroupCoordinator是可用的
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
		// 构造OffsetFetchRequest请求对象
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<>(partitions));

        // send the request with a callback
		// 暂存请求到unsent，等待发送
        return client.send(coordinator, ApiKeys.OFFSET_FETCH, request)
				// 适配响应处理器
                .compose(new OffsetFetchResponseHandler());
    }

    // 处理OffsetFetchResponse响应
    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        	// 根据响应数据构造相应大小的字典用于存放处理后的offset
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            // 遍历响应结果
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
            	// 得到TopicPartition
                TopicPartition tp = entry.getKey();
                // 得到对应的offset信息数据
                OffsetFetchResponse.PartitionData data = entry.getValue();
                // 判断是否有异常
                if (data.hasError()) {
                    Errors error = Errors.forCode(data.errorCode);
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    	// Group正在加载中
                        // just retry
                        future.raise(error);
                    } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    	// 对于Group没有对应的Coordinator
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION) {
                    	// 未知的MemberID
						// 不合法的年代信息
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(error);
                    } else {
                    	// 其他异常
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                	// 没有异常，且返回的offset数据量大于0，将其添加到offsets字典中
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }
			// 传播offset集合，最终通过fetchCommittedOffset()方法返回
            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        public MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }


}
