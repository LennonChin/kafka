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
package kafka.cluster

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
  * @param topic 代表的主题
  * @param partitionId 代表的分区编号
  * @param time
  * @param replicaManager
  */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  // 该分区所在的brokerId
  private val localBrokerId = replicaManager.config.brokerId
  // 当前broker上的LogManager对象
  private val logManager = replicaManager.logManager
  // 操作Zookeeper的辅助类
  private val zkUtils = replicaManager.zkUtils
  // 维护该分区的全部副本的集合信息（AR集合）
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  // 更新ISR使用的读写锁
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  // Zookeeper中的版本
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  // Leader副本的年代信息
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // 该分区的Leader副本的ID
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // 维护该分区的ISR集合，ISR是AR的子集
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  // 根据replicaId判断指定副本是否是本地副本，replicaId与brokerId相同说明是本地副本
  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  // 当前分区是否使用了副本
  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) => // 获取到了Leader副本
        // 因为Leader副本存在，如果此时ISR小于AR说明至少存在两个副本，即当前分区使用了副本
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  // 获取或创建Replica
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    // 根据replicaId从assignedReplicaMap中查找Replica对象
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      // 查找到了，直接返回
      case Some(replica) => replica
      case None =>
        // 未查找到，将创建
        if (isReplicaLocal(replicaId)) { // 判断是否为Local Replica
          // 获取配置信息，Zookeeper中的配置会覆盖默认的配置
          val config = LogConfig.fromProps(logManager.defaultConfig.originals, // 默认配置
                                            // 从Zookeeper中获取
                                           AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          // 使用LogManager创建对应的Log，如果对应的Log已存在会直接返回
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          // 获取HighWatermark Checkpoint，该信息存储在每个数据目录下的replication-offset-checkpoint文件中
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          // 读取replication-offset-checkpoint内的数据
          val offsetMap = checkpoint.read
          // 该Checkpoint数据中没有当前Partition的HighWatermark信息
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          // 获取对应的HighWatermark，并且与LEO进行比较，最小的那个作为此副本的HighWatermark
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          // 创建Replica对象，并添加到assignedReplicaMap集合中管理
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          // 是远程副本，直接创建Replica对象并添加到AR集合中，此Replica的log为null
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        // 返回对应的Replica
        getReplica(replicaId).get
    }
  }

  // 根据replicaId从AR集合中获取分区对象
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    // 从assignedReplicaMap集合（AR集合）中查找
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  // 判断Leader副本是否在当前分区所在的broker上，如果在就获取表示Leader副本的Replica对象
  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        // Leader副本存在于当前分区所在的broker上
        if (leaderReplicaId == localBrokerId)
          // 获取对应的Replica
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  // 将Replica添加到assignedReplicaMap集合，键为Replica的brokerId
  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  // 获取AR集合中的所有Replica对象
  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  // 根据replicaId从AR集合中移除指定的Replica对象
  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  // 删除当前分区
  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) { // 加锁
      // 将AR集合清空
      assignedReplicaMap.clear()
      // 将IST集合置为空集合
      inSyncReplicas = Set.empty[Replica]
      // 将Leader副本置为NONE
      leaderReplicaIdOpt = None
      try {
        // 使用LogManager将分区对应的数据删除
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  // 获取Leader年代信息
  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
    * 切换为Leader副本角色
   */
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 获取需要分配的AR集合
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // 记录Controller的年代信息
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 1. 创建AR集合中所有副本对应的Replica对象（如果不存在该Replica）
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      // 2. 获取ISR集合
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      // 3. 根据allReplicas更新assignedReplicas集合
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // 4. 更新Partition中的相关字段
      inSyncReplicas = newInSyncReplicas // 更新ISR
      leaderEpoch = partitionStateInfo.leaderEpoch // 更新Leader的年代信息
      zkVersion = partitionStateInfo.zkVersion // 更新Zookeeper中的版本信息
      // 5. 检测Leader是否发生变化
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          // Leader所在的broker没有发生变化
          false
        } else {
          // Leader之前并不在此broker上，表示Leader发生了变化，更新leaderReplicaIdOpt
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      // 获取本地副本
      val leaderReplica = getReplica().get
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {
        /**
          * 6. 初始化Leader的highWatermarkMetadata
          * 如果Leader副本发生了迁移，则表示Leader副本通过上面的步骤刚刚分配到此broker上
          * 可能是刚启动，也可能是Follower副本成为Leader副本，此时需要更新leaderReplica的highWatermarkMetadata
          * 由leaderReplica的convertHWToLocalOffsetMetadata()方法完成
          */
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        // 7. 重置所有远程副本的LEO为-1
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // 8. 尝试更新HighWatermark
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      // 如果HighWatermark被更新了，尝试执行DelayedFetch和DelayedProduce延迟任务
      tryCompleteDelayedRequests()
    // 返回结果表示是否发生Leader迁移
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
    *  切换为Follower副本角色
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    // 加锁
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 创建对应的Replica对象
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      // 根据partitionStateInfo信息更新AR集合
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // ISR集合在Leader副本上维护，Follower副本上不维护，因此Follower副本上的ISR会置为空集合
      inSyncReplicas = Set.empty[Replica]
      // 更新Leader年代信息
      leaderEpoch = partitionStateInfo.leaderEpoch
      // 更新Zookeeper中的版本信息
      zkVersion = partitionStateInfo.zkVersion
      // 检测Leader是否发生改变
      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      } else {
        // 更新leaderReplicaIdOpt字段
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    // 根据replicaId获取对应的Replica对象
    getReplica(replicaId) match {
      case Some(replica) =>
        // 更新副本的LEO
        replica.updateLogReadResult(logReadResult)
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        /**
          * 检查是否应该将当前副本加入In-Sync集合；有可能当前副本由于滞后被In-Sync集合排除了，
          * 此时可以执行一次检测，如果满足条件就将当前副本重新添加到In-Sync集合中
          */
        maybeExpandIsr(replicaId)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   *
   * This function can be triggered when a replica's LEO has incremented
    * ISR集合扩充
   */
  def maybeExpandIsr(replicaId: Int) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      // 检测当前Replica是否应该被添加到In-Sync集合中
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 根据replicaId获取Replica对象
          val replica = getReplica(replicaId).get
          // 查看Leader副本的HighWatermark
          val leaderHW = leaderReplica.highWatermark
          /**
            * 同时满足以下三个条件则可以将当前副本添加到In-Sync集合中：
            * 1. 当前In-Sync集合不包含当前Replica副本；
            * 2. 当前副本是否是assignedReplicas副本（AR）之一；
            * 3. 当前副本的LEO大于等于Leader副本的HighWatermark；
            */
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
                  replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            // 将当前副本添加到In-Sync集合
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            // 更新缓存及Zookeeper中的In-Sync集合数据
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }

          // check if the HW of the partition can now be incremented
          // since the replica maybe now be in the ISR and its LEO has just incremented
          // 检测分区的HighWatermark是否可以更新了，如果更新了，该方法会返回true
          maybeIncrementLeaderHW(leaderReplica)

        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      // 如果分区的HighWatermark发生了更新，尝试完成时间轮中的DelayedProduce和DelayedFetch延迟操作
      tryCompleteDelayedRequests()
  }

  /*
   * Note that this method will only be called if requiredAcks = -1
   * and we are waiting for all replicas in ISR to be fully caught up to
   * the (local) leader's offset corresponding to this produce request
   * before we acknowledge the produce request.
   * 检测HighWatermark线
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Short) = {
    leaderReplicaIfLocal() match {
      // 当前副本是Leader副本
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        // 获取当前的In-Sync副本
        val curInSyncReplicas = inSyncReplicas
        // 已经确认同步的个数，通过遍历副本，判断副本的LEO是否大于requiredOffset
        val numAcks = curInSyncReplicas.count(r => {
          if (!r.isLocal)
            // 判断副本的LEO是否大于requiredOffset
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace("Replica %d of %s-%d received offset %d".format(r.brokerId, topic, partitionId, requiredOffset))
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        })

        trace("%d acks satisfied for %s-%d with acks = -1".format(numAcks, topic, partitionId))

        // 主题配置的最小In-Sync副本数
        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        /**
          * 如果Leader的HighWatermark大于等于requiredOffset，说明所有In-Sync副本都已经完成同步了，HighWatermark更新了
          * 1. 此时如果满足了最小In-Sync副本数，直接返回true及NONE错误码即可；
          * 2. 此时如果还不满足最小In-Sync副本数，说明副本数满足不了，但同步完成了，就返回true及NOT_ENOUGH_REPLICAS_AFTER_APPEND错误码；
          * 如果Leader的HighWatermark小于requiredOffset，说明In-Sync副本未完成同步，返回false及NONE错误码。
          */
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * The topic may be configured not to accept messages if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
          */
          // 判断是否满足In-Sync副本最低要求
          if (minIsr <= curInSyncReplicas.size) {
            // 同步完成
            (true, Errors.NONE.code)
          } else {
            // 同步完成，但副本数不够
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code)
          }
        } else
          // 未同步完成
          (false, Errors.NONE.code)
      // 当前副本不是Leader副本
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION.code)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
    *
    * 在分区的ISR发生变化，或任何副本的LEO发生变化时会触发该方法更新HW
    * 当HW发生更新时返回true，否则返回false
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {
    // 获取In-Sync副本的LEO
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    // 根据所有In_Sync副本的LEO来计算HighWatermark，即取最小的LEO为HighWatermark
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    // 旧的HighWatermark
    val oldHighWatermark = leaderReplica.highWatermark
    // 新的HighWatermark比旧的HighWatermark大，或者新的HighWatermark还处于该LogSegment上
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      // 更新HighWatermark
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    // 尝试完成消息拉取请求
    replicaManager.tryCompleteDelayedFetch(requestKey)
    // 尝试完成消息写入请求
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  // ISR集合缩减
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    // 加锁
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // 获取Leader副本对应的Replica对象
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 找出滞后的Follower副本的集合
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.size > 0) {
            // 从In-Sync副本集中减去滞后的副本
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            // 更新缓存及Zookeeper中的In-Sync信息
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            // 因为更新了ISR，尝试更新Leader的HighWatermark
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    // 如果HighWatermark发生了更新，尝试完成时间轮中的DelayedProduce和DelayedFetch延迟操作
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  // 获取同步状态滞后的副本集合
  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
      *                     该副本的LEO长时间没有更新，表示该副本可能卡住了（Stuck）
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
      *                    该副本长时间没有读取到LEO的数据，表示该副本处于滞后状态
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    // Leader副本的LEO
    val leaderLogEndOffset = leaderReplica.logEndOffset
    // Follower副本集合（即除开Leader副本）
    val candidateReplicas = inSyncReplicas - leaderReplica

    // 从Follower副本集合中找出滞后的副本，通过当前时间距副本的lastCaughtUpTimeMs时间差与maxLagMs进行比较
    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.size > 0)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  // 向日志系统写入消息
  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      /**
        * 检查当前分区是否就是Leader分区，内部根据ReplicaID是否与BrokerID相同来判断
        * 如果当前分区就是Leader分区就返回该分区副本（即Leader副本），否则返回None
        * 只有Leader分区才可以处理读写请求，读写日志数据
        */
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        // 当前分区是Leader分区，并且得到了对应的副本分区
        case Some(leaderReplica) =>
          // 获取对应的Log对象
          val log = leaderReplica.log.get
          // 根据Log的配置获取最小ISR
          val minIsr = log.config.minInSyncReplicas
          // 查看当前Leader的In-Sync副本数量
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          // 如果In-Sync小于分区要求的最小ISR，且ACK要求为-1，则表示In-Sync满足不了ISR，抛出NotEnoughReplicasException异常
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }

          // 否则In-Sync是满足最小ISR的，将消息数据添加Log中
          val info = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          // 尝试完成延迟的拉取操作，这个拉取操作一般是副本的拉取操作，传入的键是以主题和分区ID组成的TopicPartitionOperationKey
          replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          // 可能需要更新HighWatermark值
          (info, maybeIncrementLeaderHW(leaderReplica))
        // 当前分区不是Leader分区，抛出NotLeaderForPartitionException异常
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      // 如果HighWatermark发生了更新，尝试完成延迟请求
      tryCompleteDelayedRequests()

    info
  }

  // 更新Zookeeper和缓存中的In-Sync集合记录
  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      // 使用ReplicaManager记录ISR的更新
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      // 更新记录的ISR
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
