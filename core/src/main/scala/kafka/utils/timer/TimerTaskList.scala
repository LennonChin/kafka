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

import java.util.concurrent.{TimeUnit, Delayed}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import kafka.utils.{SystemTime, threadsafe}

import scala.math._

/**
  * @param taskCounter 任务数量
  */
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  // root节点，其TimerTask为null，过期时间为-1
  private[this] val root = new TimerTaskEntry(null, -1)
  // 初始化时root节点的前驱和后继都指向自己
  root.next = root
  root.prev = root

  // 过期时间
  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  // 设置过期时间
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  // 获取过期时间
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  // 遍历所有的TimerTask对象，对其执行f操作
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      // 遍历
      while (entry ne root) {
        val nextEntry = entry.next
        // 任务未取消时，对其执行f操作
        if (!entry.cancelled) f(entry.timerTask)
        // 后移
        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  // 添加TimerTaskEntry
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    // 循环尝试
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      // 先将其从原TimerTaskList链中移除
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            // 将其链接到链表尾
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            // 更新Task数量计数器
            taskCounter.incrementAndGet()
            // 标识完成
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  // 从链表中移除TimerTaskEntry
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        // TimerTaskEntry中记录了自己所在的TimerTaskList链，判断是否与当前TimerTaskList是同一个
        if (timerTaskEntry.list eq this) {
          // 后继节点的前驱改为自己的前驱
          timerTaskEntry.next.prev = timerTaskEntry.prev
          // 前驱节点的后继改为自己的后继
          timerTaskEntry.prev.next = timerTaskEntry.next
          // 将自己的后继和前驱都设为null
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          // 将自己引用的当前链表设为null
          timerTaskEntry.list = null
          // 维护链表的任务个数
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  // 对所有TimerTaskEntry执行f操作并将其从链表中移除
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      // 头节点
      var head = root.next
      // 从头向尾遍历链表
      while (head ne root) {
        // 先从链表中移除
        remove(head)
        // 调用传入的方法参数进行处理
        f(head)
        // 移向下一个元素
        head = root.next
      }
      // 重置过期时间
      expiration.set(-1L)
    }
  }

  // 获取延迟时间
  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - SystemTime.milliseconds, 0), TimeUnit.MILLISECONDS)
  }

  // 比较两个TimerTaskList链，是以过期时间进行比较的
  def compareTo(d: Delayed): Int = {
    // 类型转换
    val other = d.asInstanceOf[TimerTaskList]
    // 比较过期时间
    if(getExpiration < other.getExpiration) -1
    else if(getExpiration > other.getExpiration) 1
    else 0
  }

}

/**
  * @param timerTask 任务对象
  * @param expirationMs 过期时间
  */
private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  @volatile
  // 当前TimerTaskEntry所在TimerTaskList链
  var list: TimerTaskList = null
  // 后继TimerTaskEntry
  var next: TimerTaskEntry = null
  // 前驱TimerTaskEntry
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 在TimerTask中记录TimerTaskEntry
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  // 任务是否取消
  def cancelled: Boolean = {
    // TimerTask中记录TimerTaskEntry不是自己说明取消了
    timerTask.getTimerTaskEntry != this
  }

  // 将当前TimerTaskEntry从所在的TimerTaskList链中移除
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  // 比较两个TimerTaskEntry，通过过期时间进行比较
  override def compare(that: TimerTaskEntry): Int = {
    this.expirationMs compare that.expirationMs
  }
}

