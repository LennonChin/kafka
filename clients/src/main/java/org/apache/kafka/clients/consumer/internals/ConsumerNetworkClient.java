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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Higher level consumer access to the network layer with basic support for futures and
 * task scheduling. This class is not thread-safe, except for wakeup().
 */
public class ConsumerNetworkClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerNetworkClient.class);

    // 这里使用的是NetworkClient对象
    private final KafkaClient client;
    // 由调用KafkaConsumer对象的消费者线程之外的其他线程设置，表示要中断KafkaConsumer线程
	private final AtomicBoolean wakeup = new AtomicBoolean(false);
	/**
	 * 定时任务队列，底层使用JDK的PriorityQueue实现，
	 * PriorityQueue是一个非线程安全、无界、具有优先级功能的优先队列
	 * 实现原理是小顶堆，底层基于数组
	 * PriorityBlockingQueue是PriorityQueue的线程安全实现
	 */
    private final DelayedTaskQueue delayedTasks = new DelayedTaskQueue();
    // 缓存队列，key是Node节点，value是发往该Node节点的ClientRequest集合
    private final Map<Node, List<ClientRequest>> unsent = new HashMap<>();
    // 集群元数据
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    // ClientRequest在unsent中缓存的超时时长（request.timeout.ms）
    private final long unsentExpiryMs;

    // this count is only accessed from the consumer's main thread
	/**
	 * KafkaConsumer是否正在执行不可中断的方法，该值只会被KafkaConsumer线程修改
	 * 每进入一个不可中断的方法，该值加1，退出不可中断的方法时，该值减1
	 */
    private int wakeupDisabledCount = 0;


    public ConsumerNetworkClient(KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 long requestTimeoutMs) {
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.unsentExpiryMs = requestTimeoutMs;
    }

    /**
     * Schedule a new task to be executed at the given time. This is "best-effort" scheduling and
     * should only be used for coarse synchronization.
     * @param task The task to be scheduled
     * @param at The time it should run
     */
    public void schedule(DelayedTask task, long at) {
    	// 向delayedTasks添加任务
        delayedTasks.add(task, at);
    }

    /**
     * Unschedule a task. This will remove all instances of the task from the task queue.
     * This is a no-op if the task is not scheduled.
     * @param task The task to be unscheduled.
     */
    public void unschedule(DelayedTask task) {
        delayedTasks.remove(task);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(long)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     * @param node The destination of the request
     * @param api The Kafka API call
     * @param request The request payload
     * @return A future which indicates the result of the send.
	 *
	 * 封装请求为ClientRequest，保存到unsent集合中等待发送
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              ApiKeys api,
                                              AbstractRequest request) {
        long now = time.milliseconds();
        RequestFutureCompletionHandler future = new RequestFutureCompletionHandler();
        // 获取请求头
        RequestHeader header = client.nextRequestHeader(api);
        // 创建RequestSend对象
        RequestSend send = new RequestSend(node.idString(), header, request.toStruct());
        // 添加到unsent中，等待发送
        put(node, new ClientRequest(now, true, send, future));
        return future;
    }

    private void put(Node node, ClientRequest request) {
    	// 先从unsent中获取有没有对应node的List<ClientRequest>集合
        List<ClientRequest> nodeUnsent = unsent.get(node);
        if (nodeUnsent == null) {
        	// 如果没有则创建
            nodeUnsent = new ArrayList<>();
            unsent.put(node, nodeUnsent);
        }
        // 添加ClientRequest到对应的List<ClientRequest>集合
        nodeUnsent.add(request);
    }

    public Node leastLoadedNode() {
    	// 查找Kafka集群中负载最低的Node
        return client.leastLoadedNode(time.milliseconds());
    }

    /**
     * Block until the metadata has been refreshed.
     */
    public void awaitMetadataUpdate() {
    	// 获取当前的集群元数据版本暂存
        int version = this.metadata.requestUpdate();
        // 循环调用poll操作直到集群元数据版本发生变化
        do {
            poll(Long.MAX_VALUE);
        } while (this.metadata.version() == version);
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    public void ensureFreshMetadata() {
    	// 强制更新Metadata，判断是否已经到了更新时间
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(time.milliseconds()) == 0)
        	// 更新元数据，会阻塞
            awaitMetadataUpdate();
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public void poll(RequestFuture<?> future) {
    	// 循环检测future是否完成，如果没有完成就执行poll()操作
        while (!future.isDone())
            poll(Long.MAX_VALUE);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timeout The maximum duration (in ms) to wait for the request
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long begin = time.milliseconds();
        long remaining = timeout;
        long now = begin;
        do {
            poll(remaining, now, true);
            now = time.milliseconds();
            long elapsed = now - begin;
            remaining = timeout - elapsed;
        } while (!future.isDone() && remaining > 0);
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timeout The maximum time to wait for an IO event.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds(), true);
    }

    /**
     * Poll for any network IO.
     * @param timeout timeout in milliseconds
     * @param now current time in milliseconds
     */
    public void poll(long timeout, long now) {
        poll(timeout, now, true);
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups,
     * nor will it execute any delayed tasks.
     */
    public void pollNoWakeup() {
    	// wakeupDisabledCount++
        disableWakeups();
        try {
            poll(0, time.milliseconds(), false);
        } finally {
        	// wakeupDisabledCount--
            enableWakeups();
        }
    }
	
	/**
	 *
	 * @param timeout 执行poll()方法时最长的阻塞时间，如果为0表示不阻塞
	 * @param now 调用该方法的当前时间
	 * @param executeDelayedTasks 是否执行delayedTasks队列中的定时任务
	 */
    private void poll(long timeout, long now, boolean executeDelayedTasks) {
        // send all the requests we can send now
		// 检测Node节点发送条件，循环处理unsent中缓存的请求，将发送请求绑定到KafkaChannel的send上，等待发送
        trySend(now);

        // ensure we don't poll any longer than the deadline for
        // the next scheduled task
		// 计算超时时间，取超时时间和delayedTasks队列中最近要执行的定时任务的时间的较小值
        timeout = Math.min(timeout, delayedTasks.nextTimeout(now));
        // 使用NetworkClient处理消息发送
        clientPoll(timeout, now);
        now = time.milliseconds();

        // handle any disconnects by failing the active requests. note that disconnects must
        // be checked immediately following poll since any subsequent call to client.ready()
        // will reset the disconnect status
		// 检测消费者和每个Node之间的连接状态
        checkDisconnects(now);

        // execute scheduled tasks
		// 根据executeDelayedTasks决定是否要处理delayedTasks队列中超时的定时任务
        if (executeDelayedTasks)
        	// 处理超时的定时任务
            delayedTasks.poll(now);

        // try again to send requests since buffer space may have been
        // cleared or a connect finished in the poll
		// 再次调用trySend循环处理unsent中缓存的请求
        trySend(now);

        // fail requests that couldn't be sent if they have expired
		// 处理unsent中超时的请求
        failExpiredRequests(now);
    }

    /**
     * Execute delayed tasks now.
     * @param now current time in milliseconds
     * @throws WakeupException if a wakeup has been requested
     */
    public void executeDelayedTasks(long now) {
        delayedTasks.poll(now);
        maybeTriggerWakeup();
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     */
    public void awaitPendingRequests(Node node) {
		/**
		 * pendingRequestCount()会获取对应Node中unsent暂存的请求数量与InFlightRequests中正在发送的请求数量之和
		 * 当请求还未全部完成，就一直进行poll操作
		 */
		while (pendingRequestCount(node) > 0)
            poll(retryBackoffMs);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        List<ClientRequest> pending = unsent.get(node);
        int unsentCount = pending == null ? 0 : pending.size();
        return unsentCount + client.inFlightRequestCount(node.idString());
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        int total = 0;
        for (List<ClientRequest> requests: unsent.values())
            total += requests.size();
        return total + client.inFlightRequestCount();
    }

    // 检查消费者与每个Node之间的连接状态
    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        // 遍历unsent的键值对
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            // 获取Node
            Node node = requestEntry.getKey();
            // 检查Node连接是否失败
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
				// 如果失败就将相应的键值对从unsent中移除
                iterator.remove();
                // 遍历处理所有移除的ClientRequest的回调
                for (ClientRequest request : requestEntry.getValue()) {
                    RequestFutureCompletionHandler handler =
                            (RequestFutureCompletionHandler) request.callback();
                    // 注意第三个参数为true，表示这是由于断开连接而产生的回调
                    handler.onComplete(new ClientResponse(request, now, true, null));
                }
            }
        }
    }

    // 处理unsent中超时的请求
    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        // 遍历unsent集合
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            // 获取ClientRequest集合迭代器
            Iterator<ClientRequest> requestIterator = requestEntry.getValue().iterator();
            while (requestIterator.hasNext()) {
            	// 得到ClientRequest
                ClientRequest request = requestIterator.next();
				/**
				 * 判断ClientRequest是否超时，判断方式
				 * 1. now - unsentExpiryMs：即从当前时间往前推unsentExpiryMs毫秒（request.timeout.ms）
				 * 2. 如果ClientRequest的创建时间还在这个时间之前，说明超时
				 */
				if (request.createdTimeMs() < now - unsentExpiryMs) {
					// 超时处理，使用ClientRequest的handle抛出TimeoutException异常
                    RequestFutureCompletionHandler handler =
                            (RequestFutureCompletionHandler) request.callback();
                    handler.raise(new TimeoutException("Failed to send request after " + unsentExpiryMs + " ms."));
                    // 从集合中移除
                    requestIterator.remove();
                } else
                    break;
            }
            // 如果对应的ClientRequest已经空了，就将其从unsent中移除
            if (requestEntry.getValue().isEmpty())
                iterator.remove();
        }
    }

    protected void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
		// 清空该Node对应的ClientRequest
        List<ClientRequest> unsentRequests = unsent.remove(node);
        if (unsentRequests != null) {
            for (ClientRequest request : unsentRequests) {
            	// 调用raise()方法传递异常
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                handler.raise(e);
            }
        }
    }

    // 发送unsent缓存中的ClientRequest
    private boolean trySend(long now) {
        // send any requests that can be sent now
        boolean requestsSent = false;
        // 遍历每个<Node, List<ClientRequest>>键值对
        for (Map.Entry<Node, List<ClientRequest>> requestEntry: unsent.entrySet()) {
        	// 获取对应的Node节点
            Node node = requestEntry.getKey();
            // 获取对应的ClientRequest集合迭代器
            Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
            // 遍历ClientRequest集合
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 检查节点是否可用
                if (client.ready(node, now)) {
                	// 将请求绑定到KafkaChannel上
                    client.send(request, now);
                    // 从集合中移除对应的ClientRequest
                    iterator.remove();
                    requestsSent = true;
                }
            }
        }
        return requestsSent;
    }

    private void clientPoll(long timeout, long now) {
    	// 调用NetworkClient的poll进行消息发送
        client.poll(timeout, now);
        // 检测wakeup和wakeupDisabledCount，查看是否有其他线程中断
        maybeTriggerWakeup();
    }

    private void maybeTriggerWakeup() {
        // 检测wakeup和wakeupDisabledCount，查看是否有其他线程中断
        if (wakeupDisabledCount == 0 && wakeup.get()) {
        	// 设置中断标志
            wakeup.set(false);
            // 如果有，抛出WakeupException，中断当前poll()方法操作
            throw new WakeupException();
        }
    }

    public void disableWakeups() {
        wakeupDisabledCount++;
    }

    public void enableWakeups() {
        if (wakeupDisabledCount <= 0)
            throw new IllegalStateException("Cannot enable wakeups since they were never disabled");

        wakeupDisabledCount--;

        // re-wakeup the client if the flag was set since previous wake-up call
        // could be cleared by poll(0) while wakeups were disabled
        if (wakeupDisabledCount == 0 && wakeup.get())
        	// 如果wakeupDisabledCount为0且KafkaConsumer线程需要中断，就唤醒NetworkClient发送数据
            this.client.wakeup();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * Find whether a previous connection has failed. Note that the failure state will persist until either
     * {@link #tryConnect(Node)} or {@link #send(Node, ApiKeys, AbstractRequest)} has been called.
     * @param node Node to connect to if possible
     */
    public boolean connectionFailed(Node node) {
        return client.connectionFailed(node);
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, ApiKeys, AbstractRequest)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
    	// 检查Node是否准备好，如果准备好了就尝试连接
        client.ready(node, time.milliseconds());
    }

    public static class RequestFutureCompletionHandler
            extends RequestFuture<ClientResponse>
            implements RequestCompletionHandler {
    
        /**
         * 该方法重写了RequestCompletionHandler接口的
         * RequestCompletionHandler接口只声明了这一个方法
         */
        @Override
        public void onComplete(ClientResponse response) {
            if (response.wasDisconnected()) {
            	// 如果是因为连接断开而产生的响应
                ClientRequest request = response.request();
                RequestSend send = request.request();
                ApiKeys api = ApiKeys.forId(send.header().apiKey());
                int correlation = send.header().correlationId();
                log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected",
                        api, request, correlation, send.destination());
                // 调用RequestFuture的raise()方法，传递DisconnectException异常
                raise(DisconnectException.INSTANCE);
            } else {
            	// 否则正常完成回调，该方法来自RequestFuture类
                complete(response);
            }
        }
    }
}
