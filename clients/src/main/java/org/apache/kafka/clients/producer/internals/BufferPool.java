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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {

	// 整个Pool的大小
    private final long totalMemory;
    private final int poolableSize;
    // 控制并发访问
    private final ReentrantLock lock;
    // 队列，缓存了指定大小的ByteBuffer
    private final Deque<ByteBuffer> free;
    // 记录因申请不到足够空间而阻塞的线程所在的Condition条件队列
    private final Deque<Condition> waiters;
    // 可用空间大小，totalMemory - free.size() * ByteBuffer大小
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
	 *
	 * 从缓存池中申请ByteBuffer缓存对象
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
    	// 如果申请的大小比Pool总大小还大，就抛出异常
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        // 加锁
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
			// 当请求的大小是poolableSize，且free中还有空闲
            if (size == poolableSize && !this.free.isEmpty())
            	// 从free中poll出队首ByteBuffer返回
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
			// 走到这里说明请求的大小不是poolableSize，或者大小是poolableSize但free是空的
			// 计算free中空闲的大小
            int freeListSize = this.free.size() * this.poolableSize;
            if (this.availableMemory + freeListSize >= size) {
            	// 如果剩余可用内存加上free总空闲内存是大于等于需要的大小
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
				// 使用freeUp()方法使availableMemory满足需要的大小
                freeUp(size);
                // 从availableMemory中减去需要的大小
                this.availableMemory -= size;
                lock.unlock();
                // 返回size大小的ByteBuffer对象
                return ByteBuffer.allocate(size);
            } else {
            	// 如果剩余可用内存加上free总空闲内存是小于需要的大小
                // we are out of memory and will have to block
                int accumulated = 0;
                ByteBuffer buffer = null;
                // 构造新的Condition等待队列
                Condition moreMemory = this.lock.newCondition();
                // 最大等待时间
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
				// 当申请到的缓存小于需要的缓存大小
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                    	// 等待一定时间，返回结果表示是否等待超时
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                    	// 如果发生中断，就将等待队列从waiters中移除
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                    	// 计算等待的时间
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        // 记录等待时间
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    // 判断是否等待超时
                    if (waitingTimeElapsed) {
                    	// 如果超时，就将等待队列从waiters中移除，并且抛出异常
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    // 从最大等待时间中减去已经等待的时间
                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
					// 如果accumulated为0，申请的大小是poolableSize，free不为空
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
						// 就从free中poll出队首的ByteBuffer对象
                        buffer = this.free.pollFirst();
                        // 将accumulated置为申请的大小
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
						// 这个分支会尝试先分配一部分大小，然后再下一次while循环中继续等待分配
						// 尝试使用freeUp()使availableMemory满足size - accumulated，可能是不够的
                        freeUp(size - accumulated);
                        // 查看已经得到的大小
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        // 从availableMemory中减去这部分大小
                        this.availableMemory -= got;
                        // 然后将得到的大小添加到accumulated上
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
				// 从waiters移除队首的Condition
                Condition removed = this.waiters.removeFirst();
                // 判断移除的是否是刚刚添加的
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                	// 如果availableMemory大于0，free队列不为空
                    if (!this.waiters.isEmpty())
                    	// 且waiters不为空，则将waiters队首的Condition唤醒
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
				// 解锁
                lock.unlock();
                // 分配ByteBuffer，返回
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
        	// 如果还没有解锁，就解锁
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
		// 如果free不为空，且availableMemory小于size，则一直从free中poll
		// 并将poll出的空间加到availableMemory上
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
	 * 将ByteBuffer还给缓冲池
	 * 需要注意的是size参数可能小于buffer.capacity，因为buffer可能在使用过程中重新分配了
     */
    public void deallocate(ByteBuffer buffer, int size) {
    	// 上锁
        lock.lock();
        try {
        	// size是poolableSize大小，且size与buffer的容量相同
            if (size == this.poolableSize && size == buffer.capacity()) {
            	// 清空buffer，并将buffer添加到free队列中
                buffer.clear();
                this.free.add(buffer);
            } else {
				// 否则就不归还buffer到size中，而是将size大小加到availableMemory上
				this.availableMemory += size;
            }
            // 归还之后，获取waiters队首的Condition条件队列，并唤醒该条件队列上等待的线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
        	// 解锁
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
