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

package kafka.utils

import java.util.concurrent._
import atomic._
import org.apache.kafka.common.utils.Utils

/**
 * A scheduler for running jobs
 * 
 * This interface controls a job scheduler that allows scheduling either repeating background jobs 
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
trait Scheduler {
  
  /**
   * Initialize this scheduler so it is ready to accept scheduling of tasks
   */
  def startup()
  
  /**
   * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur. 
   * This includes tasks scheduled with a delayed execution.
   */
  def shutdown()
  
  /**
   * Check if the scheduler has been started
   */
  def isStarted: Boolean
  
  /**
   * Schedule a task
   * @param name The name of this task
   * @param delay The amount of time to wait before the first execution
   * @param period The period with which to execute the task. If < 0 the task will execute only once.
   * @param unit The unit for the preceding times.
   */
  def schedule(name: String, fun: ()=>Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS)
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * 
 * It has a pool of kafka-scheduler- threads that do the actual work.
 * 
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
@threadsafe
class KafkaScheduler(val threads: Int, 
                     val threadNamePrefix: String = "kafka-scheduler-", 
                     daemon: Boolean = true) extends Scheduler with Logging {
  // JDK定时任务线程池
  private var executor: ScheduledThreadPoolExecutor = null
  private val schedulerThreadId = new AtomicInteger(0)

  override def startup() {
    debug("Initializing task scheduler.")
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!")
      // 构造器内初始化定时任务线程池
      // 配置线程数
      executor = new ScheduledThreadPoolExecutor(threads)
      // 配置在线程池关闭后放弃执行已存在的周期任务
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
      // 配置在线程池关闭后放弃执行已存在的定时任务
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      // 配置线程工厂
      executor.setThreadFactory(new ThreadFactory() {
                                  // 默认创建是守护线程
                                  def newThread(runnable: Runnable): Thread = 
                                    Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon)
                                })
    }
  }
  
  override def shutdown() {
    debug("Shutting down task scheduler.")
    // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
    val cachedExecutor = this.executor
    if (cachedExecutor != null) {
      this synchronized {
        cachedExecutor.shutdown()
        this.executor = null
      }
      // 等待一天，需要等待剩余已存在的周期任务执行完成
      cachedExecutor.awaitTermination(1, TimeUnit.DAYS)
    }
  }

  def schedule(name: String, fun: ()=>Unit, delay: Long, period: Long, unit: TimeUnit) = {
    debug("Scheduling task %s with initial delay %d ms and period %d ms."
        .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      // 保证线程池已经启动
      ensureRunning
      // 封装fun为Runnable对象
      val runnable = CoreUtils.runnable {
        try {
          trace("Beginning execution of scheduled task '%s'.".format(name))
          // 主要的执行任务代码
          fun()
        } catch {
          case t: Throwable => error("Uncaught exception in scheduled task '" + name +"'", t)
        } finally {
          trace("Completed execution of scheduled task '%s'.".format(name))
        }
      }
      if(period >= 0)
        // 提交周期任务
        executor.scheduleAtFixedRate(runnable, delay, period, unit)
      else
        // 提交非周期任务
        executor.schedule(runnable, delay, unit)
    }
  }
  
  def isStarted: Boolean = {
    this synchronized {
      executor != null
    }
  }
  
  private def ensureRunning = {
    if(!isStarted)
      throw new IllegalStateException("Kafka scheduler is not running.")
  }
}
