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

// TimerTask特质
trait TimerTask extends Runnable {

  // 延迟时间
  val delayMs: Long // timestamp in millisecond

  // 包装TimerTask的TimerTaskEntry
  private[this] var timerTaskEntry: TimerTaskEntry = null

  // 取消TimerTask
  def cancel(): Unit = {
    synchronized {
      // 将当前任务的timerTaskEntry从TimerTaskList中移除
      if (timerTaskEntry != null) timerTaskEntry.remove()
      // 将所在的TimerTaskEntry置为null
      timerTaskEntry = null
    }
  }

  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      // 先将旧的TimerTaskEntry从TimerTaskList链移除
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      // 然后进行设置
      timerTaskEntry = entry
    }
  }

  // 获取当前TimerTask的TimerTaskEntry
  private[timer] def getTimerTaskEntry(): TimerTaskEntry = {
    timerTaskEntry
  }

}
