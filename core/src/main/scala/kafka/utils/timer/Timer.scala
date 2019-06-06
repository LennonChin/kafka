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
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = System.currentTimeMillis) extends Timer {

  // timeout timer
  // JDK线程池，固定线程数为1，自定义了线程工厂
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      Utils.newThread("executor-"+executorName, runnable, false)
  })

  // 各个层级的时间轮共用的DelayQueue队列，主要作用是阻塞推进时间轮表针的线程（ExpiredOperationReaper），等待最近到期任务到期
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 各个层级时间轮共用的任务个数计数器
  private[this] val taskCounter = new AtomicInteger(0)
  // 层级时间轮中最底层的时间轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  // 用来同步时间轮指针currentTime修改的读写锁
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  // 添加定时任务
  def add(timerTask: TimerTask): Unit = {
    // 加读锁
    readLock.lock()
    try {
      // 进行添加
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + System.currentTimeMillis()))
    } finally {
      readLock.unlock()
    }
  }

  // 添加定时任务
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 向时间轮中添加任务
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 如果向时间轮中添加失败，可能是因为任务过期或已经取消了
      if (!timerTaskEntry.cancelled)
        // 如果任务是因为过期被拒，则直接放置到线程池中执行
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  // 将timerTaskEntry重新添加到时间轮中
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 推进时间轮指针，同时对到期的TimerTaskList中的任务进行处理。
   * 如果TimerTaskList到期，但是其中的某些任务未到期，会将未到期任务进行降级，添加到低层次的时间轮中继续等待；
   * 如果任务到期了，则提交到taskExecutor线程池中执行。
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    /**
      * 从队列中取出第一个TimerTaskList元素，会阻塞，等待超时时间为timeoutMs
      * 注意，这里取出的TimerTaskList的过期时间是到期的，也就是说该链表中的任务已经到执行时间了
      * 如果没有到期的TimerTaskList将在阻塞等待超时后返回null
      */
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) { // 取出TimerTaskList元素不为空
      // 加写锁
      writeLock.lock()
      try {
        while (bucket != null) { // 重新检查取出的TimerTaskList元素
          // 根据TimerTaskList的过期时间推进时间轮指针
          timingWheel.advanceClock(bucket.getExpiration())
          /**
            * 调用TimerTaskList对象的flush()方法重新处理其中的任务
            * 该操作会将bucket中所有元素移除来然后重新添加到时间轮
            * 在重新添加的操作中如果遇到到期的任务会交给taskExecutor线程池执行
            * 也有可能对其进行降级到下层时间轮
            * 注意，在这个重新添加的过程中，到期的任务是会被直接提交到线程池执行的
            * 可以回顾 addTimerTaskEntry() 方法
            */
          bucket.flush(reinsert)
          // 再次从delayQueue中取一个TimerTaskList元素，不会阻塞
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

