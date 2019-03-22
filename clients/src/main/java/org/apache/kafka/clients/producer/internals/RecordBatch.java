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
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);
	// 记录保存的Record数量
    public int recordCount = 0;
    // 最大Record的字节数
    public int maxRecordSize = 0;
    // 尝试发送当前RecordBatch的次数
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    // 最后一次尝试发送的时间戳
    public long lastAttemptMs;
    // 存放消息的对象，指向用来存储数据的MemoryRecords对象
    public final MemoryRecords records;
    // 当前RecordBatch中缓存的消息都会发送给次TopicPartition
    public final TopicPartition topicPartition;
    // 标识RecordBatch状态的Future对象
    public final ProduceRequestResult produceFuture;
    // 最后一次向RecordBatch追加消息的时间戳
    public long lastAppendTime;
    private final List<Thunk> thunks;
    // 用来记录某消息在RecordBatch中的偏移量
    private long offsetCounter = 0L;
    // 是否正在重试
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
    	// 估算剩余空间是否足够
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
        	// 向MemoryRecords中添加数据，offsetCounter是在RecordBatch中的偏移量
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);
            // 记录最大消息大小的字节数，这个值会不断更新，始终记录已添加的消息记录中最大的那条的大小
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            // 更新最近添加时间
            this.lastAppendTime = now;
            // 将消息构造一个FutureRecordMetadata对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            // 如果callback不会空，就将上面得到的FutureRecordMetadata和该callback包装为一个thunk，放到thunks集合里
            if (callback != null)
                thunks.add(new Thunk(callback, future));
            // 更新保存的记录数量
            this.recordCount++;
            // 返回FutureRecordMetadata对象
            return future;
        }
    }

    /**
     * Complete the request
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp The timestamp returned by the broker.
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition,
                  baseOffset,
                  exception);
        // execute callbacks
		// 循环所有的thunks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
            	// 取出thunk
                Thunk thunk = this.thunks.get(i);
                if (exception == null) {
                	// 无错误的情况
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
					// 构造RecordMetadata对象
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition,  baseOffset, thunk.future.relativeOffset(),
                                                                 timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                                                                 thunk.future.checksum(),
                                                                 thunk.future.serializedKeySize(),
                                                                 thunk.future.serializedValueSize());
                    // 调用thunk中callback回调的onCompletion()方法，exception参数传null
                    thunk.callback.onCompletion(metadata, null);
                } else {
                	// 有错误的情况，metadata传null，传入exception
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        // 调用produceFuture的done()方法，标识整个RecordBatch已经完成处理
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
    	// 指向对应消息的Callback对象，callback用于在消息发送后的回调
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            expire = true;
        else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs)))
            expire = true;
        else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs)))
            expire = true;

        if (expire) {
            this.records.close();
            this.done(-1L, Record.NO_TIMESTAMP, new TimeoutException("Batch containing " + recordCount + " record(s) expired due to timeout while requesting metadata from brokers for " + topicPartition));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
