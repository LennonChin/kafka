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

package kafka.server

import scala.collection.mutable
import scala.collection.Set
import scala.collection.Map
import kafka.utils.Logging
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Utils

abstract class AbstractFetcherManager(protected val name: String, clientId: String, numFetchers: Int = 1)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher
  // 用于管理AbstractFetcherThread对象；
  // BrokerAndFetcherId类型的键中封装了broker的网络位置信息（brokerId、host、port等）以及对应的Fetcher线程的ID
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, AbstractFetcherThread]
  private val mapLock = new Object
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  // 根据主题和分区ID计算拉取线程ID
  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicAndPartition, BrokerAndInitialOffset]) {
    mapLock synchronized { // 加锁
      /**
        * 对partitionAndOffsets根据构造的BrokerAndFetcherId为分组项进行分组
        * 每个Fetcher线程只服务于一个broker，但可以为多个分区的Follower完成同步
        * 得到的结果为 Map[BrokerAndFetcherId, Seq[Map[TopicAndPartition, BrokerAndInitialOffset]]]
        */
      val partitionsPerFetcher = partitionAndOffsets.groupBy{ case(topicAndPartition, brokerAndInitialOffset) =>
        BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition))}
      // 遍历得到的partitionsPerFetcher
      for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
        var fetcherThread: AbstractFetcherThread = null
        // 根据brokerAndFetcherId取出对应的Fetcher线程，即每个broker应该有一个对应的Fetcher线程
        fetcherThreadMap.get(brokerAndFetcherId) match {
          // 取到了就赋值给fetcherThread
          case Some(f) => fetcherThread = f
          // 没取到，创建一个新的Fetcher线程，将其添加到fetcherThreadMap，并启动该线程
          case None =>
            fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
            fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
            fetcherThread.start
        }

        // 将分区信息以及同步起始位置传递给Fetcher线程，并唤醒Fetcher线程，开始同步
        fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (topicAndPartition, brokerAndInitOffset) =>
          topicAndPartition -> brokerAndInitOffset.initOffset
        })
      }
    }

    info("Added fetcher for partitions %s".format(partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
      "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
  }

  // 移除某些分区的拉取任务
  def removeFetcherForPartitions(partitions: Set[TopicAndPartition]) {
    mapLock synchronized { // 加锁
      // 遍历fetcherThreadMap，移除对应的任务
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions)
      }
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
  }

  // 关闭空闲拉取线程
  def shutdownIdleFetcherThreads() {
    mapLock synchronized { // 加锁
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
      // 遍历fetcherThreadMap
      for ((key, fetcher) <- fetcherThreadMap) {
        // 如果Fetcher线程没有为任何Follower副本进行同步，就将其停止
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          // 记录停止Fetcher线程的broker
          keysToBeRemoved += key
        }
      }
      // 从fetcherThreadMap中移除对应的记录
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  // 关闭所有管理的拉取线程
  def closeAllFetchers() {
    mapLock synchronized {
      // 遍历，调用shutdown()
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      // 清空fetcherThreadMap
      fetcherThreadMap.clear()
    }
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class BrokerAndInitialOffset(broker: BrokerEndPoint, initOffset: Long)
