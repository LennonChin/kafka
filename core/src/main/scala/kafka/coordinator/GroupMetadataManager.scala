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

package kafka.coordinator

import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.CoreUtils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.protocol.types.Type.STRING
import org.apache.kafka.common.protocol.types.Type.INT32
import org.apache.kafka.common.protocol.types.Type.INT64
import org.apache.kafka.common.protocol.types.Type.BYTES
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Time
import org.apache.kafka.clients.consumer.ConsumerRecord
import kafka.utils._
import kafka.common._
import kafka.message._
import kafka.log.FileMessageSet
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import kafka.common.MessageFormatter
import kafka.server.ReplicaManager
import scala.collection._
import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.internals.TopicConstants

case class DelayedStore(messageSet: Map[TopicPartition, MessageSet],
                        callback: Map[TopicPartition, PartitionResponse] => Unit)

/**
  * 负责管理Consumer Group元数据以及其对应offset信息的组件
  * @param brokerId
  * @param config
  * @param replicaManager 用于管理Leader副本、ISR集合、AR集合、Leader副本的迁移等
  * @param zkUtils
  * @param time
  */
class GroupMetadataManager(val brokerId: Int,
                           val config: OffsetConfig,
                           replicaManager: ReplicaManager,
                           zkUtils: ZkUtils,
                           time: Time) extends Logging with KafkaMetricsGroup {

  /** offsets cache
    * 记录每个Consumer Group消费的分区的offset位置
    **/
  private val offsetsCache = new Pool[GroupTopicPartition, OffsetAndMetadata]

  /** group metadata cache
    * 记录每个Consumer Group在服务端对应的GroupMetadata对象
    **/
  private val groupsCache = new Pool[String, GroupMetadata]

  /** partitions of consumer groups that are being loaded, its lock should be always called BEFORE offsetExpireLock and the group lock if needed
    * 记录正在加载的Offset Topic分区的ID
    **/
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /** partitions of consumer groups that are assigned, using the same loading partition lock
    * 记录已经加载的Offsets Topic分区的ID
    **/
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* lock for expiring stale offsets, it should be always called BEFORE the group lock if needed */
  private val offsetExpireLock = new ReentrantReadWriteLock()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /** number of partitions for the consumer metadata topic
    * 记录Offsets Topic的分区数量
    **/
  private val groupMetadataTopicPartitionCount = getOffsetsTopicPartitionCount

  /** Single-thread scheduler to handling offset/group metadata cache loading and unloading
    * 用于执行delete-expired-consumer-offsets、GroupCoordinator迁移等任务
    **/
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  this.logIdent = "[Group Metadata Manager on Broker " + brokerId + "]: "

  // 启动定时任务调度器
  scheduler.startup()
  /**
    * 提交delete-expired-consumer-offsets定时任务，调用的是deleteExpiredOffsets方法
    * 用于定时对offsetsCache集合进行删除操作
    * 执行周期默认为600秒，由offsets.retention.check.interval.ms项配置
    */
  scheduler.schedule(name = "delete-expired-consumer-offsets",
    fun = deleteExpiredOffsets,
    period = config.offsetsRetentionCheckIntervalMs,
    unit = TimeUnit.MILLISECONDS)

  newGauge("NumOffsets",
    new Gauge[Int] {
      def value = offsetsCache.size
    }
  )

  newGauge("NumGroups",
    new Gauge[Int] {
      def value = groupsCache.size
    }
  )

  def currentGroups(): Iterable[GroupMetadata] = groupsCache.values

  // 对Group ID取模，得到Consumer Group对应的Offsets Topic分区编号
  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  /**
    * 在处理JoinGroupRequest、OffsetFetchRequest、OffsetCommitRequest及HeartbeatRequest这些请求之前，
    * 都会调用isGroupLocal()和isGroupLoading()两个方法进行检测，如果检测失败，则直接返回异常响应
    */
  // 检测当前GroupCoordinator是否管理指定的Consumer Group
  def isGroupLocal(groupId: String): Boolean = loadingPartitions synchronized ownedPartitions.contains(partitionFor(groupId))

  // 检测指定的Consumer Group对应的Offsets Topic分区是否还处于加载过程中
  def isGroupLoading(groupId: String): Boolean = loadingPartitions synchronized loadingPartitions.contains(partitionFor(groupId))

  def isLoading(): Boolean = loadingPartitions synchronized !loadingPartitions.isEmpty

  /**
   * Get the group associated with the given groupId, or null if not found
    *
    * 根据groupId从groupsCache获取对应的GroupMetadata
   */
  def getGroup(groupId: String): GroupMetadata = {
      groupsCache.get(groupId)
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
    *
    * 添加GroupMetadata到groupsCache
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    // 当不存在时才能添加
    val currentGroup = groupsCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup // 添加不成功返回已存在的GroupMetadata
    } else {
      group // 否则返回添加的GroupMetadata
    }
  }

  /**
   * Remove all metadata associated with the group
   * @param group
   */
  def removeGroup(group: GroupMetadata) {
    // guard this removal in case of concurrent access (e.g. if a delayed join completes with no members
    // while the group is being removed due to coordinator emigration)
    if (groupsCache.remove(group.groupId, group)) { // 删除groupsCache中对应的GroupMetadata
      // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
      // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
      // retry removing this group.
      // 获取Consumer Group在Offsets Topic中对应的分区的ID
      val groupPartition = partitionFor(group.groupId)
      // 获取当前对应的魔数和时间戳
      val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(groupPartition)
      // 产生"删除标记"消息，消息值为null，键由groupId封装而来
      val tombstone = new Message(bytes = null, key = GroupMetadataManager.groupMetadataKey(group.groupId),
        timestamp = timestamp, magicValue = magicValue)

      // 获取Offsets Topic中该Consumer Group对应的Partition对象
      val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, groupPartition)
      partitionOpt.foreach { partition => // 存在Partition对象
        val appendPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, groupPartition)

        trace("Marking group %s as deleted.".format(group.groupId))

        try {
          // do not need to require acks since even if the tombstone is lost,
          // it will be appended again by the new leader
          // TODO KAFKA-2720: periodic purging instead of immediate removal of groups
          // 使用Partition对象的appendMessagesToLeader(...)方法写入消息
          partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, tombstone))
        } catch {
          case t: Throwable =>
            error("Failed to mark group %s as deleted in %s.".format(group.groupId, appendPartition), t)
          // ignore and continue
        }
      }
    }
  }

  // 根据Consumer Group Leader的分区分配结果创建消息
  def prepareStoreGroup(group: GroupMetadata,
                        groupAssignment: Map[String, Array[Byte]],
                        responseCallback: Short => Unit): DelayedStore = {
    // 获取__consumer_offsets中指定Consumer Group的消息数据的魔数和时间戳
    val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(partitionFor(group.groupId))
    // 构造消息对象
    val message = new Message(
      key = GroupMetadataManager.groupMetadataKey(group.groupId), // 键，包含了Group ID
      bytes = GroupMetadataManager.groupMetadataValue(group, groupAssignment), // 值，包含了分配结果
      timestamp = timestamp, // 时间戳
      magicValue = magicValue) // 魔数

    // 以主题为__consumer_offsets，分区为Group ID对应的分区，创建TopicPartition对象
    val groupMetadataPartition = new TopicPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))

    // 构建消息字典，表示Offsets Topic分区与消息集合的对应关系
    val groupMetadataMessageSet = Map(groupMetadataPartition ->
      new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, message))

    // 当前Consumer Group的年代信息
    val generationId = group.generationId

    // set the callback function to insert the created group into cache after log append completed
    // 回调函数，会在上述消息成功追加到Offsets Topic对应的分区之后被调用
    def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || ! responseStatus.contains(groupMetadataPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, groupMetadataPartition))

      // construct the error status in the propagated assignment response
      // in the cache
      val status = responseStatus(groupMetadataPartition)

      var responseCode = Errors.NONE.code
      if (status.errorCode != Errors.NONE.code) {
        debug("Metadata from group %s with generation %d failed when appending to log due to %s"
          .format(group.groupId, generationId, Errors.forCode(status.errorCode).exceptionName))

        // transform the log append error code to the corresponding the commit status error code
        responseCode = if (status.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code) {
          Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code
        } else if (status.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code) {
          Errors.NOT_COORDINATOR_FOR_GROUP.code
        } else if (status.errorCode == Errors.REQUEST_TIMED_OUT.code) {
          Errors.REBALANCE_IN_PROGRESS.code
        } else if (status.errorCode == Errors.MESSAGE_TOO_LARGE.code
          || status.errorCode == Errors.RECORD_LIST_TOO_LARGE.code
          || status.errorCode == Errors.INVALID_FETCH_SIZE.code) {

          error("Appending metadata message for group %s generation %d failed due to %s, returning UNKNOWN error code to the client"
            .format(group.groupId, generationId, Errors.forCode(status.errorCode).exceptionName))

          Errors.UNKNOWN.code
        } else {
          error("Appending metadata message for group %s generation %d failed due to unexpected error: %s"
            .format(group.groupId, generationId, status.errorCode))

          status.errorCode
        }
      }

      responseCallback(responseCode)
    }

    // 以消息数据、回调函数构造一个DelayedStore对象
    DelayedStore(groupMetadataMessageSet, putCacheCallback)
  }

  // 实现追加消息的操作
  def store(delayedAppend: DelayedStore) {
    // call replica manager to append the group message
    replicaManager.appendMessages(
      config.offsetCommitTimeoutMs.toLong,
      config.offsetCommitRequiredAcks,
      true, // allow appending to internal offset topic
      delayedAppend.messageSet,
      delayedAppend.callback)
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  def prepareStoreOffsets(groupId: String,
                          consumerId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit): DelayedStore = {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (topicPartition, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    // construct the message set to append
    val messages = filteredOffsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
      val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(partitionFor(groupId))
      new Message(
        key = GroupMetadataManager.offsetCommitKey(groupId, topicAndPartition.topic, topicAndPartition.partition),
        bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata),
        timestamp = timestamp,
        magicValue = magicValue
      )
    }.toSeq

    val offsetTopicPartition = new TopicPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionFor(groupId))

    val offsetsAndMetadataMessageSet = Map(offsetTopicPartition ->
      new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages:_*))

    // set the callback function to insert offsets into cache after log append completed
    def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || ! responseStatus.contains(offsetTopicPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, offsetTopicPartition))

      // construct the commit response status and insert
      // the offset and metadata to cache if the append status has no error
      val status = responseStatus(offsetTopicPartition)

      val responseCode =
        if (status.errorCode == Errors.NONE.code) {
          filteredOffsetMetadata.foreach { case (topicAndPartition, offsetAndMetadata) =>
            putOffset(GroupTopicPartition(groupId, topicAndPartition), offsetAndMetadata)
          }
          Errors.NONE.code
        } else {
          debug("Offset commit %s from group %s consumer %s with generation %d failed when appending to log due to %s"
            .format(filteredOffsetMetadata, groupId, consumerId, generationId, Errors.forCode(status.errorCode).exceptionName))

          // transform the log append error code to the corresponding the commit status error code
          if (status.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code
          else if (status.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code)
            Errors.NOT_COORDINATOR_FOR_GROUP.code
          else if (status.errorCode == Errors.MESSAGE_TOO_LARGE.code
            || status.errorCode == Errors.RECORD_LIST_TOO_LARGE.code
            || status.errorCode == Errors.INVALID_FETCH_SIZE.code)
            Errors.INVALID_COMMIT_OFFSET_SIZE.code
          else
            status.errorCode
        }


      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
        if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
          (topicAndPartition, responseCode)
        else
          (topicAndPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
      }

      // finally trigger the callback logic passed from the API layer
      responseCallback(commitStatus)
    }

    DelayedStore(offsetsAndMetadataMessageSet, putCacheCallback)
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(group: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    trace("Getting offsets %s for group %s.".format(topicPartitions, group))

    if (isGroupLocal(group)) {
      if (topicPartitions.isEmpty) {
        // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
        offsetsCache.filter(_._1.group == group).map { case(groupTopicPartition, offsetAndMetadata) =>
          (groupTopicPartition.topicPartition, new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE.code))
        }.toMap
      } else {
        topicPartitions.map { topicPartition =>
          val groupTopicPartition = GroupTopicPartition(group, topicPartition)
          (groupTopicPartition.topicPartition, getOffset(groupTopicPartition))
        }.toMap
      }
    } else {
      debug("Could not fetch offsets for group %s (not offset coordinator).".format(group))
      topicPartitions.map { topicPartition =>
        val groupTopicPartition = GroupTopicPartition(group, topicPartition)
        (groupTopicPartition.topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NOT_COORDINATOR_FOR_GROUP.code))
      }.toMap
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def loadGroupsForPartition(offsetsPartition: Int,
                             onGroupLoaded: GroupMetadata => Unit) {
    // 向KafkaScheduler提交一个线程任务
    val topicPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    scheduler.schedule(topicPartition.toString, loadGroupsAndOffsets) // 任务调用了loadGroupsAndOffsets()方法

    def loadGroupsAndOffsets() {
      info("Loading offsets and group metadata from " + topicPartition)

      // 检查指定的Offsets Topic的分区是否正在加载
      loadingPartitions synchronized {
        if (loadingPartitions.contains(offsetsPartition)) {
          // 正在加载，直接返回
          info("Offset load from %s already in progress.".format(topicPartition))
          return
        } else {
          // 否则将该分区的ID添加到loadingPartitions中
          loadingPartitions.add(offsetsPartition)
        }
      }

      val startMs = time.milliseconds()
      try {
        replicaManager.logManager.getLog(topicPartition) match { // 得到该分区的对应的Log对象
          case Some(log) => // 能获取到对应的Log对象
            var currOffset = log.logSegments.head.baseOffset
            val buffer = ByteBuffer.allocate(config.loadBufferSize) // 默认为5MB
            // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
            inWriteLock(offsetExpireLock) {
              // 构造需要加载的GroupMetadata字典
              val loadedGroups = mutable.Map[String, GroupMetadata]()
              // 构造需要移除的GroupMetadata对应的groupId集合
              val removedGroups = mutable.Set[String]()

              // 持续读取HighWatermark线之前的消息
              while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {
                buffer.clear()
                // 读取消息数据到buffer中
                val messages = log.read(currOffset, config.loadBufferSize).messageSet.asInstanceOf[FileMessageSet]
                messages.readInto(buffer, 0)
                // 构造为ByteBufferMessageSet对象
                val messageSet = new ByteBufferMessageSet(buffer)
                // 对该ByteBufferMessageSet对象进行遍历
                messageSet.foreach { msgAndOffset =>
                  require(msgAndOffset.message.key != null, "Offset entry key should not be null")

                  // 读取并解析消息的键
                  val baseKey = GroupMetadataManager.readMessageKey(msgAndOffset.message.key)

                  if (baseKey.isInstanceOf[OffsetKey]) { // 键是表示Offset位置的键
                    // load offset
                    // 转换键的key字段为GroupTopicPartition对象，其中保存了Group ID、主题名称和分区ID
                    val key = baseKey.key.asInstanceOf[GroupTopicPartition]
                    if (msgAndOffset.message.payload == null) { // 消息的值为null，表示该消息是"删除标记"
                      // 从offsetsCache中移除相应的键对应的OffsetAndMetadata对象
                      if (offsetsCache.remove(key) != null)
                        trace("Removed offset for %s due to tombstone entry.".format(key))
                      else
                        trace("Ignoring redundant tombstone for %s.".format(key))
                    } else {
                      // special handling for version 0:
                      // set the expiration time stamp as commit time stamp + server default retention time
                      // 消息的值不为null，表示该消息不是"删除标记"，需要将消息解析为OffsetAndMetadata对象，存入到offsetsCache中
                      val value = GroupMetadataManager.readOffsetMessageValue(msgAndOffset.message.payload)
                      // 将相应的键和解析得到的OffsetAndMetadata对象存入到offsetsCache中
                      putOffset(key, value.copy (
                        expireTimestamp = {
                          if (value.expireTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP) // 为-1时
                            value.commitTimestamp + config.offsetsRetentionMs // offset过期时间，默认为24*60*60*1000，即24小时
                          else // 不为-1
                            value.expireTimestamp
                        }
                      ))
                      trace("Loaded offset %s for %s.".format(value, key))
                    }
                  } else { // 键是表示GroupMetadata的键
                    // load group metadata
                    // 转换键的key字段为String对象，即对应的Group ID
                    val groupId = baseKey.key.asInstanceOf[String]
                    // 将消息的值解析为GroupMetadata对象
                    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, msgAndOffset.message.payload)
                    if (groupMetadata != null) { // 解析得到的GroupMetadata对象不为空，表示该消息有效，需要写入到groupsCache
                      trace(s"Loaded group metadata for group ${groupMetadata.groupId} with generation ${groupMetadata.generationId}")
                      // 先移除removedGroups中记录了该GroupID
                      removedGroups.remove(groupId)
                      // 在将新的GroupMetadata添加到loadedGroups
                      loadedGroups.put(groupId, groupMetadata)
                    } else { // 解析得到的GroupMetadata对象为空，表示该消息为"删除标记"，需要将其从groupsCache中移除
                      // 从loadedGroups中移除
                      loadedGroups.remove(groupId)
                      // 添加到removedGroups中
                      removedGroups.add(groupId)
                    }
                  }

                  currOffset = msgAndOffset.nextOffset
                }
              }

              // 处理loadedGroups，主要是将其中的GroupMetadata对象添加到groupsCache中
              loadedGroups.values.foreach { group =>
                val currentGroup = addGroup(group)
                if (group != currentGroup)
                  debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
                    s"because there is already a cached group with generation ${currentGroup.generationId}")
                else
                  // 触发onGroupLoaded回调
                  onGroupLoaded(group)
              }

              // 处理removedGroups，主要是将groupsCache中对应的GroupMetadata对象移除
              removedGroups.foreach { groupId =>
                val group = groupsCache.get(groupId)
                if (group != null)
                  throw new IllegalStateException(s"Unexpected unload of acitve group ${group.groupId} while " +
                    s"loading partition ${topicPartition}")
              }
            }

            if (!shuttingDown.get())
              info("Finished loading offsets from %s in %d milliseconds."
                .format(topicPartition, time.milliseconds() - startMs))
          case None =>
            warn("No log found for " + topicPartition)
        }
      }
      catch {
        case t: Throwable =>
          error("Error in loading offsets from " + topicPartition, t)
      }
      finally {
        // 将当前Offset Topic分区的id从loadingPartitions集合移入ownedPartitions集合，标识该分区加载完成
        loadingPartitions synchronized {
          ownedPartitions.add(offsetsPartition)
          loadingPartitions.remove(offsetsPartition)
        }
      }
    }
  }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               onGroupUnloaded: GroupMetadata => Unit) {
    // 向KafkaScheduler提交一个线程任务
    val topicPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    scheduler.schedule(topicPartition.toString, removeGroupsAndOffsets) // 任务调用了removeGroupsAndOffsets()方法

    def removeGroupsAndOffsets() {
      // 记录offset和group被移除的数量
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      loadingPartitions synchronized {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        // 从ownedPartitions中移除对应的Offsets Topic的分区，标识当前GroupCoordinator不再管理其对应Consumer Group
        ownedPartitions.remove(offsetsPartition)

        // clear the offsets for this partition in the cache

        /**
         * NOTE: we need to put this in the loading partition lock as well to prevent race condition of the leader-is-local check
         * in getOffsets to protects against fetching from an empty/cleared offset cache (i.e., cleared due to a leader->follower
         * transition right after the check and clear the cache), causing offset fetch return empty offsets with NONE error code
          *
          * 遍历offsetsCache字典的键集
         */
        offsetsCache.keys.foreach { key =>
          if (partitionFor(key.group) == offsetsPartition) {
            offsetsCache.remove(key) // 将对应的OffsetAndMetadata全部清除
            // 计数
            numOffsetsRemoved += 1
          }
        }

        // clear the groups for this partition in the cache
        // 遍历groupsCache字典的值集
        for (group <- groupsCache.values) {
          if (partitionFor(group.groupId) == offsetsPartition) {
            // 调用回调函数
            onGroupUnloaded(group)
            // 将对应的GroupMetadata全部清除
            groupsCache.remove(group.groupId, group)
            // 计数
            numGroupsRemoved += 1
          }
        }
      }

      if (numOffsetsRemoved > 0) info("Removed %d cached offsets for %s on follower transition."
        .format(numOffsetsRemoved, TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)))

      if (numGroupsRemoved > 0) info("Removed %d cached groups for %s on follower transition."
        .format(numGroupsRemoved, TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)))
    }
  }

  /**
   * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
    *
    * 根据Group ID、主题名称和分区ID获取对应的offset
   *
   * @param key The requested group-topic-partition
   * @return If the key is present, return the offset and metadata; otherwise return None
   */
  private def getOffset(key: GroupTopicPartition): OffsetFetchResponse.PartitionData = {
    val offsetAndMetadata = offsetsCache.get(key)
    if (offsetAndMetadata == null)
      // 无对应的数据，返回INVALID_OFFSET（值为-1）
      new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE.code)
    else
      // 否则返回对应的OffsetAndMetadata中记录的数据
      new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE.code)
  }

  /**
   * Put the (already committed) offset for the given group/topic/partition into the cache.
    *
    * 根据Group ID、主题名称及分区ID添加对应的OffsetAndMetadata信息
   *
   * @param key The group-topic-partition
   * @param offsetAndMetadata The offset/metadata to be stored
   */
  private def putOffset(key: GroupTopicPartition, offsetAndMetadata: OffsetAndMetadata) {
    // 直接添加到offsetsCache中，如果存在会被替换
    offsetsCache.put(key, offsetAndMetadata)
  }

  // 删除offsetsCache中过期的OffsetMetadata对象，并向Offsets Topic中追加"删除标记"消息
  private def deleteExpiredOffsets() {
    debug("Collecting expired offsets.")

    // 当前时间
    val startMs = time.milliseconds()

    val numExpiredOffsetsRemoved = inWriteLock(offsetExpireLock) {
      // 得到offsetsCache中所有过期的OffsetAndMetadata对象
      val expiredOffsets = offsetsCache.filter { case (groupTopicPartition, offsetAndMetadata) =>
        // 所有过期时间标记小于当前时间，即认定为过期
        offsetAndMetadata.expireTimestamp < startMs
      }

      debug("Found %d expired offsets.".format(expiredOffsets.size))

      // delete the expired offsets from the table and generate tombstone messages to remove them from the log
      // 对过期offsetAndMetadata进行Map操作
      val tombstonesForPartition = expiredOffsets.map { case (groupTopicAndPartition, offsetAndMetadata) =>
        // 找到过期OffsetAndMetadata对应的分区的ID
        val offsetsPartition = partitionFor(groupTopicAndPartition.group)
        trace("Removing expired offset and metadata for %s: %s".format(groupTopicAndPartition, offsetAndMetadata))

        // 先从offsetsCache中将该过期OffsetAndMetadata对象移除
        offsetsCache.remove(groupTopicAndPartition)

        // 获取该OffsetAndMetadata对应的Group ID在__consumer_offsets主题中存储的offset位置消息的键
        val commitKey = GroupMetadataManager.offsetCommitKey(groupTopicAndPartition.group,
          groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition)

        // 获取消息对应的魔数和时间戳
        val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(offsetsPartition)
        // 构造对应的值为null的空消息
        (offsetsPartition, new Message(bytes = null, key = commitKey, timestamp = timestamp, magicValue = magicValue))
      }.groupBy { case (partition, tombstone) => partition } // 按照Offsets Topic的分区ID进行分组

      // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
      // if we crash or leaders move) since the new leaders will get rid of expired offsets during their own purge cycles.
      tombstonesForPartition.flatMap { case (offsetsPartition, tombstones) =>
        // 根据主题名称__consumer_offsets及分区ID获取对应的Partition对象
        val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
        partitionOpt.map { partition =>
          val appendPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
          // 获取要追加的"删除标记"的消息集合
          val messages = tombstones.map(_._2).toSeq

          trace("Marked %d offsets in %s for deletion.".format(messages.size, appendPartition))

          try {
            // do not need to require acks since even if the tombstone is lost,
            // it will be appended again in the next purge cycle
            // 追加"删除标记"消息，此处将messages转换为了多参数序列
            partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages: _*))
            tombstones.size
          }
          catch {
            case t: Throwable =>
              error("Failed to mark %d expired offsets for deletion in %s.".format(messages.size, appendPartition), t)
              // ignore and continue
              0
          }
        }
      }.sum
    }

    info("Removed %d expired offsets in %d milliseconds.".format(numExpiredOffsetsRemoved, time.milliseconds() - startMs))
  }

  private def getHighWatermark(partitionId: Int): Long = {
    val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionId)

    val hw = partitionOpt.map { partition =>
      partition.leaderReplicaIfLocal().map(_.highWatermark.messageOffset).getOrElse(-1L)
    }.getOrElse(-1L)

    hw
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }

  def shutdown() {
    shuttingDown.set(true)
    scheduler.shutdown()

    // TODO: clear the caches
  }

  /**
   * Gets the partition count of the offsets topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getOffsetsTopicPartitionCount = {
    val topic = TopicConstants.GROUP_METADATA_TOPIC_NAME
    val topicData = zkUtils.getPartitionAssignmentForTopics(Seq(topic))
    if (topicData(topic).nonEmpty)
      topicData(topic).size
    else
      config.offsetsTopicNumPartitions
  }

  private def getMessageFormatVersionAndTimestamp(partition: Int): (Byte, Long) = {
    // 构建主题为__consumer_offsets、分区ID为传入的partition参数的TopicAndPartition对象
    val groupMetadataTopicAndPartition = new TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partition)
    // 使用ReplicaManager获取魔数
    val messageFormatVersion = replicaManager.getMessageFormatVersion(groupMetadataTopicAndPartition).getOrElse {
      throw new IllegalArgumentException(s"Message format version for partition $groupMetadataTopicPartitionCount not found")
    }
    // 根据魔数版本决定时间戳
    val timestamp = if (messageFormatVersion == Message.MagicValue_V0) Message.NoTimestamp else time.milliseconds()
    (messageFormatVersion, timestamp)
  }

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  def addPartitionOwnership(partition: Int) {
    loadingPartitions synchronized {
      ownedPartitions.add(partition)
    }
  }
}

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * key version 2:       group metadata
 *     -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
object GroupMetadataManager {

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_METADATA_V0 = new Schema(new Field("member_id", STRING),
    new Field("client_id", STRING),
    new Field("client_host", STRING),
    new Field("session_timeout", INT32),
    new Field("subscription", BYTES),
    new Field("assignment", BYTES))
  private val MEMBER_METADATA_MEMBER_ID_V0 = MEMBER_METADATA_V0.get("member_id")
  private val MEMBER_METADATA_CLIENT_ID_V0 = MEMBER_METADATA_V0.get("client_id")
  private val MEMBER_METADATA_CLIENT_HOST_V0 = MEMBER_METADATA_V0.get("client_host")
  private val MEMBER_METADATA_SESSION_TIMEOUT_V0 = MEMBER_METADATA_V0.get("session_timeout")
  private val MEMBER_METADATA_SUBSCRIPTION_V0 = MEMBER_METADATA_V0.get("subscription")
  private val MEMBER_METADATA_ASSIGNMENT_V0 = MEMBER_METADATA_V0.get("assignment")


  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(new Field("protocol_type", STRING),
    new Field("generation", INT32),
    new Field("protocol", STRING),
    new Field("leader", STRING),
    new Field("members", new ArrayOf(MEMBER_METADATA_V0)))
  private val GROUP_METADATA_PROTOCOL_TYPE_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("protocol_type")
  private val GROUP_METADATA_GENERATION_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("generation")
  private val GROUP_METADATA_PROTOCOL_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("protocol")
  private val GROUP_METADATA_LEADER_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("leader")
  private val GROUP_METADATA_MEMBERS_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("members")

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1)
  private val CURRENT_OFFSET_VALUE_SCHEMA_VERSION = 1.toShort

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(0 -> GROUP_METADATA_VALUE_SCHEMA_V0)
  private val CURRENT_GROUP_VALUE_SCHEMA_VERSION = 0.toShort

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)

  private val CURRENT_OFFSET_VALUE_SCHEMA = schemaForOffset(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
  private val CURRENT_GROUP_VALUE_SCHEMA = schemaForGroup(CURRENT_GROUP_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForOffset(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroup(version: Int) = {
    val schemaOpt = GROUP_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
    *
    * 用于创建记录消费offset位置的消息的键，由groupId、主题名称、分区ID组成
   *
   * @return key for offset commit message
   */
  private def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group) // groupId
    key.set(OFFSET_KEY_TOPIC_FIELD, topic) // 主题名称
    key.set(OFFSET_KEY_PARTITION_FIELD, partition) // 分区ID

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer) // 写入ByteBuffer
    byteBuffer.array() // 转换为数组返回
  }

  /**
   * Generates the key for group metadata message for given group
    *
    * 用于创建记录GroupMetadata的消息的键，仅有groupId一个字段组成
   *
   * @return key bytes for group metadata message
   */
  private def groupMetadataKey(group: String): Array[Byte] = {
    val key = new Struct(CURRENT_GROUP_KEY_SCHEMA)
    key.set(GROUP_KEY_GROUP_FIELD, group) // 设置Group ID

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer) // 将键写入到ByteBuffer中
    byteBuffer.array() // 转换为数组并返回
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @return payload for offset commit message
   */
  private def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata): Array[Byte] = {
    // generate commit value with schema version 1
    val value = new Struct(CURRENT_OFFSET_VALUE_SCHEMA)
    value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
    value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
    value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
    value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1, offsetAndMetadata.expireTimestamp)
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata
   * @return payload for offset commit message
   */
  private def groupMetadataValue(groupMetadata: GroupMetadata, assignment: Map[String, Array[Byte]]): Array[Byte] = {
    // generate commit value with schema version 1
    val value = new Struct(CURRENT_GROUP_VALUE_SCHEMA)
    value.set(GROUP_METADATA_PROTOCOL_TYPE_V0, groupMetadata.protocolType)
    value.set(GROUP_METADATA_GENERATION_V0, groupMetadata.generationId)
    value.set(GROUP_METADATA_PROTOCOL_V0, groupMetadata.protocol)
    value.set(GROUP_METADATA_LEADER_V0, groupMetadata.leaderId)

    val memberArray = groupMetadata.allMemberMetadata.map {
      case memberMetadata =>
        val memberStruct = value.instance(GROUP_METADATA_MEMBERS_V0)
        memberStruct.set(MEMBER_METADATA_MEMBER_ID_V0, memberMetadata.memberId)
        memberStruct.set(MEMBER_METADATA_CLIENT_ID_V0, memberMetadata.clientId)
        memberStruct.set(MEMBER_METADATA_CLIENT_HOST_V0, memberMetadata.clientHost)
        memberStruct.set(MEMBER_METADATA_SESSION_TIMEOUT_V0, memberMetadata.sessionTimeoutMs)

        val metadata = memberMetadata.metadata(groupMetadata.protocol)
        memberStruct.set(MEMBER_METADATA_SUBSCRIPTION_V0, ByteBuffer.wrap(metadata))

        val memberAssignment = assignment(memberMetadata.memberId)
        assert(memberAssignment != null)

        memberStruct.set(MEMBER_METADATA_ASSIGNMENT_V0, ByteBuffer.wrap(memberAssignment))

        memberStruct
    }

    value.set(GROUP_METADATA_MEMBERS_V0, memberArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) { // <= 1
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      // 记录了Version、Group ID、Topic名称和分区ID
      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) { // == 2
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      // 记录了Version和Group ID
      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException("Unknown version " + version + " for group metadata message")
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if(buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffset(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version")
      }
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadatafrom it
   *
   * @param buffer input byte-buffer
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer): GroupMetadata = {
    if(buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroup(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val protocolType = value.get(GROUP_METADATA_PROTOCOL_TYPE_V0).asInstanceOf[String]

        val group = new GroupMetadata(groupId, protocolType)

        group.generationId = value.get(GROUP_METADATA_GENERATION_V0).asInstanceOf[Int]
        group.leaderId = value.get(GROUP_METADATA_LEADER_V0).asInstanceOf[String]
        group.protocol = value.get(GROUP_METADATA_PROTOCOL_V0).asInstanceOf[String]

        value.getArray(GROUP_METADATA_MEMBERS_V0).foreach {
          case memberMetadataObj =>
            val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
            val memberId = memberMetadata.get(MEMBER_METADATA_MEMBER_ID_V0).asInstanceOf[String]
            val clientId = memberMetadata.get(MEMBER_METADATA_CLIENT_ID_V0).asInstanceOf[String]
            val clientHost = memberMetadata.get(MEMBER_METADATA_CLIENT_HOST_V0).asInstanceOf[String]
            val sessionTimeout = memberMetadata.get(MEMBER_METADATA_SESSION_TIMEOUT_V0).asInstanceOf[Int]
            val subscription = Utils.toArray(memberMetadata.get(MEMBER_METADATA_SUBSCRIPTION_V0).asInstanceOf[ByteBuffer])

            val member = new MemberMetadata(memberId, groupId, clientId, clientHost, sessionTimeout,
              List((group.protocol, subscription)))

            member.assignment = Utils.toArray(memberMetadata.get(MEMBER_METADATA_ASSIGNMENT_V0).asInstanceOf[ByteBuffer])

            group.add(memberId, member)
        }

        group
      } else {
        throw new IllegalStateException("Unknown group metadata message version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value)).toString
          output.write(groupId.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

}

// 维护Consumer Group与分区的消费关系
case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short
  def key: Object
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString = key
}

