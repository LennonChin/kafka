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
package org.apache.kafka.clients;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * 
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);
    // 两次刷新元数据退避时间，避免频繁刷新导致性能消耗
    private final long refreshBackoffMs;
    // 每隔多久更新一次，默认是300秒（metadata.max.age.ms）
    private final long metadataExpireMs;
    // 集群元数据版本号，元数据更新成功一次，版本号就自增1
    private int version;
    // 上一次更新元数据的时间戳
    private long lastRefreshMs;
	/**
	 * 上一次成功更新元数据的时间戳，如果每次更新都成功，
	 * lastSuccessfulRefreshMs应该与lastRefreshMs相同，否则lastRefreshMs > lastSuccessfulRefreshMs
	 */
	private long lastSuccessfulRefreshMs;
	// 记录kafka集群的元数据
    private Cluster cluster;
    // 表示是否强制更新Cluster
    private boolean needUpdate;
    // 记录当前已知的所有的主题
    private final Set<String> topics;
    // 监听器集合，用于监听Metadata更新
    private final List<Listener> listeners;
    // 是否需要更新全部主题的元数据
    private boolean needMetadataForAllTopics;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashSet<String>();
        this.listeners = new ArrayList<>();
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata
     */
    public synchronized void add(String topic) {
        topics.add(topic);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
		/**
		 * 元数据是否过期，判断条件：
		 * 1. needUpdate被置为true
		 * 2. 上次更新时间距离当前时间已经超过了指定的元数据过期时间阈值metadataExpireMs（metadata.max.age.ms），默认是300秒
		 */
		long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
		/**
		 * 允许更新的时间点，计算方式：
		 * 上次更新时间 + 退避时间 - 当前时间的间隔
		 * 即要求上次更新时间与当前时间的间隔不能大于退避时间，如果大于则需要等待
		 */
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true; // 设置为需要强制更新
        return this.version; // 返回当前集群元数据的版本号
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        // 比较版本号。当Sender成功更新Metadata之后，version值会加1，否则会一直循环，直到超时
        while (this.version <= lastVersion) {
            if (remainingWaitMs != 0)
            	// 带有超时机制的线程wait
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            // 超时，抛出超时异常
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.containsAll(topics))
            requestUpdate();
        this.topics.clear();
        this.topics.addAll(topics);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<String>(this.topics);
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.contains(topic);
    }

    /**
     * Update the cluster metadata
     */
    public synchronized void update(Cluster cluster, long now) {
        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        // 通知所有的监听器，数据要更新了
        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        // Do this after notifying listeners as subscribed topics' list can be changed by listeners
        // 更新Cluster字段
        this.cluster = this.needMetadataForAllTopics ? getClusterForCurrentTopics(cluster) : cluster;

        // 唤醒等待Metadata更新完成的线程
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
    	// 更新失败的情况下，只会更新lastRefreshMs字段
        this.lastRefreshMs = now;
    }
    
    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    // 根据传入的Cluster对象更新数据
    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        if (cluster != null) {
        	// 记录未授权的主题
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            // 从未授权的主题中移除当前已知可用的主题
            unauthorizedTopics.retainAll(this.topics);
			
            // 更新partition信息
            for (String topic : this.topics) {
                partitionInfos.addAll(cluster.partitionsForTopic(topic));
            }
            nodes = cluster.nodes();
        }
        // 构造新的Cluster
        return new Cluster(nodes, partitionInfos, unauthorizedTopics);
    }
}
