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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetFetchResponse, JoinGroupRequest}

import scala.collection.{Map, Seq, immutable}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 *
  * @param brokerId
  * @param groupConfig 记录Consumer Group中Consumer Session过期的最小时长和最大时长，这个区间是消费者指定的超时时长的合法区间
  * @param offsetConfig 记录OffsetMetadata相关的配置项（metadata字段允许的最大长度、Offsets Topic中每个分区的副本个数等）
  * @param groupManager
  * @param heartbeatPurgatory 用于管理DelayedHeartbeat
  * @param joinPurgatory 用于管理DelayedJoin
  * @param time
  */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  // 处理JoinGroupRequest
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    if (!isActive.get) { // 检查GroupCoordinator是否在运行
      // 调用回调函数，返回错误码GROUP_COORDINATOR_NOT_AVAILABLE
      responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!validGroupId(groupId)) { // 检查是否提供了Group ID
      // 调用回调函数，返回错误码INVALID_GROUP_ID
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 调用回调函数，返回错误码NOT_COORDINATOR_FOR_GROUP
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) { // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 调用回调函数，返回错误码GROUP_LOAD_IN_PROGRESS
      responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) { // 检查超时时间是否合法，不能超出GroupConfig中配置的超时时长区间
      // 调用回调函数，返回错误码INVALID_SESSION_TIMEOUT
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
    } else { // 各类检查合格
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      // 尝试从GroupMetadataManager的groupsCache集合中获取Group ID对应的GroupMetadata
      var group = groupManager.getGroup(groupId)
      if (group == null) { // 获取的GroupMetadata为空
        if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) { // MemberID不为空，返回UNKNOWN_MEMBER_ID错误
          responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
        } else { // MemberID为空，表示该Group ID是新的，需要创建GroupMetadata
          // 创建新的GroupMetadata并存入GroupMetadataManager的groupsCache集合
          group = groupManager.addGroup(new GroupMetadata(groupId, protocolType))
          // 进行后续处理
          doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
        }
      } else { // 获取的GroupMetadata不为空，直接进行后续处理
        doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  // 处理JoinGroupRequest
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized { // 加锁
      /**
        * 第1个分支的检测：检测Member支持的PartitionAssignor，
        *     需要检测每个消费者支持的PartitionAssignor集合与GroupMetadata中的候选PartitionAssignor集合（即candidateProtocols字段）是否有交集。
        * 第2个分支的检测：检测memberId，
        *     JoinGroupRequest可能是来自Consumer Group中已知的Member，此时请求会携带之前被分配过的memberId，这里就要检测memberId是否能被GroupMetadata识别
        */
      if (group.protocolType != protocolType || !group.supportsProtocols(protocols.map(_._1).toSet)) {
        // if the new member does not support the group protocol, reject it
        // 调用回调函数，返回INCONSISTENT_GROUP_PROTOCOL错误码
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        // 调用回调函数，返回UNKNOWN_MEMBER_ID错误码
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
      } else {
        // 根据Group当前的状态进行分别处理
        group.currentState match {
          case Dead => // Dead状态，直接返回UNKNOWN_MEMBER_ID错误码
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))

          case PreparingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // 未知的新Member申请加入，则创建Member并分配memberId，并加入GroupMetadata中
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              // 已知Member重新申请加入，则更新GroupMetadata中记录的Member信息
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case AwaitingSync =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // 未知的新Member申请加入，则创建Member并分配memberId，并加入GroupMetadata中
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else { // 已知Member重新申请加入，要区分Member支持的PartitionAssignor是否发生了变化
              // 获取对应的MemberMetadata对象
              val member = group.get(memberId)
              if (member.matches(protocols)) { // 未发生变化，将当前Member集合信息返回给Group Leader
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) { // 发送JoinGroupRequest的是Group Leader
                    // 直接返回MemberMetadata
                    group.currentMemberMetadata
                  } else {
                    // 否则返回空字典
                    Map.empty
                  },
                  memberId = memberId, // Member ID
                  generationId = group.generationId, // 当前Group的年代信息
                  subProtocol = group.protocol, // 当前Group的PartitionAssignor
                  leaderId = group.leaderId, // 当前Group的Leader ID
                  errorCode = Errors.NONE.code))
              } else {
                // member has changed metadata, so force a rebalance
                // 发生变化，更新Member信息；将状态切换为PreparingRebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Stable =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) { // 未知的新Member申请加入
              // if the member id is unknown, register the member to the group
              // 创建Member并分配memberId，并加入GroupMetadata中，将状态切换为PreparingRebalance
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else { // 已知Member重新申请加入，要区分Member支持的PartitionAssignor是否发生了变化
              val member = group.get(memberId)
              if (memberId == group.leaderId || !member.matches(protocols)) { // 发生变化或者发送JoinGroupRequest请求的是Group Leader
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                // 更新Member信息，将状态切换为PreparingRebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else { // 未发生变化
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                // 将GroupMetadata的当前状态返回，消费者会发送SyncGroupRequest继续后面的操作
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId, // Member ID
                  generationId = group.generationId, // 当前Group的年代信息
                  subProtocol = group.protocol, // 当前Group的PartitionAssignor
                  leaderId = group.leaderId, // 当前Group的Leader ID
                  errorCode = Errors.NONE.code))
              }
            }
        }

        // 根据当前的状态决定是否尝试完成相关的DelayedJoin操作
        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  // 处理SyncGroupRequest请求
  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) { // 检查GroupCoordinator是否在运行
      // 调用回调函数，返回错误码GROUP_COORDINATOR_NOT_AVAILABLE
      responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 调用回调函数，返回错误码NOT_COORDINATOR_FOR_GROUP
      responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else { // 检查通过
      // 获取对应的GroupMetadata
      val group = groupManager.getGroup(groupId)
      if (group == null)
        // GroupMetadata为空，直接返回
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      else
        // 否则使用doSyncGroup()方法处理
        doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    var delayedGroupStore: Option[DelayedStore] = None

    group synchronized {
      if (!group.has(memberId)) { // 检测Member是否为此Consumer Group的成员
        // 返回UNKNOWN_MEMBER_ID错误码
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (generationId != group.generationId) { // 检测generationId是否合法，以屏蔽来自旧Member成员的请求
        // 返回ILLEGAL_GENERATION错误码
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
      } else {
        group.currentState match { // 根据不同的Group状态分别处理
          case Dead =>
            // 直接返回UNKNOWN_MEMBER_ID错误码
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)

          case PreparingRebalance =>
            // 返回REBALANCE_IN_PROGRESS错误码
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)

          case AwaitingSync =>
            // 设置对应的MemberMetadata的awaitingSyncCallback为回调函数
            group.get(memberId).awaitingSyncCallback = responseCallback
            // 完成之前相关的DelayedHeartbeat并创建新的DelayedHeartbeat对象等待下次心跳到来
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 只有在收到的SyncGroupRequest是Group Leader发送的才会处理
            if (memberId == group.leaderId) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              // 将未分配到分区的Member对应的分配结果填充为空的Byte数组
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              // 通过GroupMetadataManager将GroupMetadata相关信息形成消息，并写入到对应的Offsets Topic分区中
              // 第三个参数是回调函数，会在消息被追加后被调用
              delayedGroupStore = Some(groupManager.prepareStoreGroup(group, assignment, (errorCode: Short) => {
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  // 检测Consumer Group状态及年代信息
                  if (group.is(AwaitingSync) && generationId == group.generationId) {
                    if (errorCode != Errors.NONE.code) {
                      // 有错误的情况，清空分区的分配结果，发送异常响应
                      resetAndPropagateAssignmentError(group, errorCode)
                      // 切换成PreparingRebalance状态
                      maybePrepareRebalance(group)
                    } else {
                      // 正常的情况，发送正常的SyncGroupResponse
                      setAndPropagateAssignment(group, assignment)
                      // 转换为Stable状态
                      group.transitionTo(Stable)
                    }
                  }
                }
              }))
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            // 将分配给此Member的负责处理的分区信息返回
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE.code)
            // 完成之前相关的DelayedHeartbeat并创建新的DelayedHeartbeat对象等待下次心跳到来
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }

    // store the group metadata without holding the group lock to avoid the potential
    // for deadlock when the callback is invoked
    // 使用GroupCoordinatorManager的store()方法进行处理
    delayedGroupStore.foreach(groupManager.store)
  }

  def handleLeaveGroup(groupId: String, consumerId: String, responseCallback: Short => Unit) {
    if (!isActive.get) { // 检查GroupCoordinator是否在运行
      // 调用回调函数，返回错误码GROUP_COORDINATOR_NOT_AVAILABLE
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 调用回调函数，返回错误码NOT_COORDINATOR_FOR_GROUP
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) { // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 调用回调函数，返回错误码GROUP_LOAD_IN_PROGRESS
      responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
    } else { // 检查通过
      // 获取对应的GroupMetadata
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        // GroupMetadata为空，调用回调函数，返回错误码UNKNOWN_MEMBER_ID
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        // GroupMetadata不为空，要分别处理
        group synchronized {
          if (group.is(Dead)) {
            // GroupMetadata状态为Dead，返回UNKNOWN_MEMBER_ID错误码
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.has(consumerId)) {
            // GroupMetadata不管理该MemberID，返回UNKNOWN_MEMBER_ID错误码
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else {
            // 获取对应的MemberMetadata
            val member = group.get(consumerId)
            // 将对应MemberMetadata的isLeaving字段设置为true，尝试完成相应的DelayedHeartbeat
            removeHeartbeatForLeavingMember(group, member)
            // 调用onMemberFailure()方法移除对应的MemberMetadata对象并完成状态变化
            onMemberFailure(group, member)
            // 调用回调函数
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  // 处理HeartbeatRequest请求的方法
  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) { // 判断当前GroupCoordinator是否允许
      // 调用回调函数，返回GROUP_COORDINATOR_NOT_AVAILABLE错误码
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 调用回调函数，返回NOT_COORDINATOR_FOR_GROUP错误码
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) { // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 调用回调函数，返回NONE错误码
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE.code)
    } else { // 检查通过
      // 获得对应的GroupMetadata
      val group = groupManager.getGroup(groupId)
      if (group == null) { // GroupMetadata为空，表示对应的分组不存在
        // 返回UNKNOWN_MEMBER_ID错误码
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        // 对应的分组存在
        group synchronized {
          if (group.is(Dead)) { // 状态为Dead，返回UNKNOWN_MEMBER_ID错误码
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.is(Stable)) { // 状态不为Stable，返回REBALANCE_IN_PROGRESS错误码
            responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
          } else if (!group.has(memberId)) { // 分组内并没有该Member，返回UNKNOWN_MEMBER_ID错误码
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (generationId != group.generationId) { // 分组年代信息不对，返回ILLEGAL_GENERATION错误码
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else { // 检查通过
            // 获取对应的MemberMetadata
            val member = group.get(memberId)
            // 调度下一次心跳定时任务
            completeAndScheduleNextHeartbeatExpiration(group, member)
            // 调用回调函数
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  /**
    * 处理客户端提交的offset
    * @param groupId 消费者组ID
    * @param memberId 消费者的Member ID
    * @param generationId 请求关联ID
    * @param offsetMetadata offset提交数据
    * @param responseCallback 回调函数
    */
  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    var delayedOffsetStore: Option[DelayedStore] = None

    if (!isActive.get) { // 判断当前GroupCoordinator是否允许
      // 调用回调函数，返回GROUP_COORDINATOR_NOT_AVAILABLE错误码
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 调用回调函数，返回NOT_COORDINATOR_FOR_GROUP错误码
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) { // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 调用回调函数，返回GROUP_LOAD_IN_PROGRESS错误码
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else { // 正常情况
      // 获取Consumer Group对应的GroupMetadata对象
      val group = groupManager.getGroup(groupId)
      if (group == null) { // GroupMetadata不存在
        // 如果对应的GroupMetadata对象不存在且generationId < 0，则表示GroupCoordinator不维护Consumer Group的分区分配结果，只记录提交的offset信息
        if (generationId < 0) // generationId小于0，说明该Consumer Group不依赖Kafka做分区管理，允许提交
          // the group is not relying on Kafka for partition management, so allow the commit
          delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId, offsetMetadata,
            responseCallback))
        else
          // the group has failed over to this coordinator (which will be handled in KAFKA-2017),
          // or this is a request coming from an older generation. either way, reject the commit
          // 否则可能是因为请求来自旧的GroupCoordinator年代，调用回调函数，返回ILLEGAL_GENERATION错误码
          responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else { // GroupMetadata存在
        group synchronized {
          if (group.is(Dead)) { // GroupMetadata状态为Dead
            // 调用回调函数，返回UNKNOWN_MEMBER_ID错误码
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (group.is(AwaitingSync)) { // GroupMetadata状态为AwaitingSync
            // 调用回调函数，返回REBALANCE_IN_PROGRESS错误码
            responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
          } else if (!group.has(memberId)) { // GroupMetadata不包含该MemberID
            // 调用回调函数，返回UNKNOWN_MEMBER_ID错误码
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (generationId != group.generationId) { // 年代信息不匹配
            // 调用回调函数，返回ILLEGAL_GENERATION错误码
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else {
            // 获取Member对应的MemberMetadata对象
            val member = group.get(memberId)
            // 准备下一次的心跳延迟任务
            completeAndScheduleNextHeartbeatExpiration(group, member)
            // 使用GroupMetadataManager提交该offset并在完成后处理回调
            delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId,
              offsetMetadata, responseCallback))
          }
        }
      }
    }

    // store the offsets without holding the group lock
    delayedOffsetStore.foreach(groupManager.store)
  }

  // 处理获取分区offset操作
  def handleFetchOffsets(groupId: String,
                         partitions: Seq[TopicPartition]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    if (!isActive.get) { // 判断当前GroupCoordinator是否正在运行
      // 未运行将记录GROUP_COORDINATOR_NOT_AVAILABLE错误码
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))}.toMap
    } else if (!isCoordinatorForGroup(groupId)) { // 判断当前GroupCoordinator是否负责管理该Consumer Group
      // 将记录NOT_COORDINATOR_FOR_GROUP错误码
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NOT_COORDINATOR_FOR_GROUP.code))}.toMap
    } else if (isCoordinatorLoadingInProgress(groupId)) { // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 将记录GROUP_LOAD_IN_PROGRESS错误码
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_LOAD_IN_PROGRESS.code))}.toMap
    } else { // 验证通过，使用GroupMetadataManager的getOffsets()方法获取对应的offset
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      groupManager.getOffsets(groupId, partitions)
    }
  }

  // 获取所有的Consumer Group
  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) { // 检查GroupCoordinator运行状态
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      // 如果存在正在加载的Offset Topic分区，则无法返回某些Consumer Group，直接返回GROUP_LOAD_IN_PROGRESS错误码
      val errorCode = if (groupManager.isLoading()) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
      // 否则获取GroupMetadataManager的groupsCache的值集，构造为GroupOverview对象列表进行返回
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        (Errors.NONE, GroupCoordinator.DeadGroup)
      } else {
        group synchronized {
          (Errors.NONE, group.summary)
        }
      }
    }
  }

  /**
    * 在handleGroupEmigration()方法中传入GroupMetadataManager.removeGroupsForPartition()方法的回调函数，
    * 它会在GroupMetadata被删除前，将Consumer Group状态转换成Dead，并根据之前的Consumer Group状态进行相应的清理操作
    */
  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      // 记录旧状态
      val previousState = group.currentState
      // 转换为Dead状态
      group.transitionTo(Dead)

      // 根据Consumer Group之前的状态进行清理
      previousState match {
        case Dead =>
        case PreparingRebalance =>
          // 调用所有MemberMetadata的awaitingJoinCallback回调函数，返回NOT_COORDINATOR_FOR_GROUP错误码
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
              // 清空awaitingJoinCallback回调函数
              member.awaitingJoinCallback = null
            }
          }
          // awaitingJoinCallback的变化，可能导致DelayedJoin满足条件，故进行尝试
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          // 调用所有MemberMetadata的awaitingJoinCallback回调函数，返回NOT_COORDINATOR_FOR_GROUP错误码
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
              // 清空awaitingSyncCallback回调函数
              member.awaitingSyncCallback = null
            }
            // awaitingSyncCallback的变化，可能导致DelayedHeartbeat满足条件，故进行尝试
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  /**
    * 在handleGroupImmigration()方法中传入GroupMetadataManager.loadGroupsForPartition()方法的回调函数
    * 当出现GroupMetadata重复加载时，会调用它更新心跳
    */
  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable))
      // 更新所有Member的心跳操作
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    // 更新GroupMetadata中每个相关的MemberMetadata.assignment字段
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))

    /**
      * 调用每个MemberMetadata的awaitingSyncCallback回调函数，创建SyncGroupResponse对象并添加到RequestChannel中等待发送；
      * 将本次心跳延迟任务完成并开始下次等待心跳的延迟任务的执行或超时
      */
    propagateAssignment(group, Errors.NONE.code)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, errorCode: Short) {
    // 检查GroupMetadata的状态是否是AwaitingSync
    assert(group.is(AwaitingSync))
    // 清空GroupMetadata中的分配情况
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, errorCode)
  }

  private def propagateAssignment(group: GroupMetadata, errorCode: Short) {
    for (member <- group.allMemberMetadata) { // 遍历所有的Member
      if (member.awaitingSyncCallback != null) {
        // 调用awaitingSyncCallback回调函数
        member.awaitingSyncCallback(member.assignment, errorCode)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        // 开启等待下次心跳的延迟任务
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
    JoinGroupResult(
      members=Map.empty,
      memberId=memberId,
      generationId=0,
      subProtocol=GroupCoordinator.NoProtocol,
      leaderId=GroupCoordinator.NoLeader,
      errorCode=errorCode)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    // 更新最后的心跳到达时间为当前时间
    member.latestHeartbeat = time.milliseconds()
    // 构造以Group ID和Member ID构成的键
    val memberKey = MemberKey(member.groupId, member.memberId)
    // 检查该键对应的Watchers，尝试完成该Watchers中的DelayedOperation
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    // 下一次心跳的过期时间
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    // 创建心跳延迟任务，并提交到heartbeatPurgatory炼狱
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  // 将对应MemberMetadata的isLeaving字段设置为true，尝试完成相应的DelayedHeartbeat
  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    // 将MemberMetadata的isLeaving设置为true
    member.isLeaving = true
    // 尝试完成相应的DelayedHeartbeat
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  // 负责添加Member信息
  private def addMemberAndRebalance(sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // use the client-id with a random id suffix as the member-id
    // 生产Member ID为"客户端ID-UUID随机字符串"
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    // 创建MemberMetadata对象
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, sessionTimeoutMs, protocols)
    // 设置MemberMetadata对象的awaitingJoinCallback为响应回调
    member.awaitingJoinCallback = callback
    // 将MemberMetadata添加到GroupMetadata中
    group.add(member.memberId, member)
    // 将状态切换为PreparingRebalance
    maybePrepareRebalance(group)
    member
  }

  // 负责更新Member信息
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    // 更新MemberMetadata支持的PartitionAssignor的协议和awaitingJoinCallback回调
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    // 尝试进行状态切换到PreparingRebalance
    maybePrepareRebalance(group)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized { // 加锁
      // 只有在State和AwaitingSync的状态下才可以切换到PreparingRebalance状态
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  // 切换状态PreparingRebalance
  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(AwaitingSync)) // 如果是AwaitingSync状态
      // 重置MemberMetadata.assignment字段，调用awaitingSyncCallback向消费者返回REBALANCE_IN_PROGRESS的错误码
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS.code)

    // 切换为PreparingRebalance状态，表示准备执行Rebalance操作
    group.transitionTo(PreparingRebalance)
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

    // DelayedJoin的超时时长是GroupMetadata中所有Member设置的超时时长的最大值
    val rebalanceTimeout = group.rebalanceTimeout
    // 创建DelayedJoin对象
    val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)
    // 创建DelayedJoin的Watcher Key
    val groupKey = GroupKey(group.groupId)
    // 尝试立即完成DelayedJoin，如果不能完成就添加到joinPurgatory炼狱
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  // 使Member下线
  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    group.remove(member.memberId) // 将Member从对应的GroupMetadata中移除
    group.currentState match { // 根据GroupMetadata状态进行处理
      // 误操作
      case Dead =>
      // 之前的分区分配可能已经失效了，将GroupMetadata切换成PreparingRebalance状态
      case Stable | AwaitingSync => maybePrepareRebalance(group)
      // GroupMetadata中的Member减少，可能满足DelayedJoin的执行条件，尝试执行
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      // 判断已知的Member是否已经申请加入
      if (group.notYetRejoinedMembers.isEmpty) // 不为空表示还有Member没有加入Group
        // 尝试强制完成
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  // 当已知Member都已申请重新加入或DelayedJoin到期时执行该方法
  def onCompleteJoin(group: GroupMetadata) {
    group synchronized { // 加锁
      // 获取未重新加入的已知Member集合
      val failedMembers = group.notYetRejoinedMembers
      if (group.isEmpty || !failedMembers.isEmpty) {
        failedMembers.foreach { failedMember =>
          group.remove(failedMember.memberId) // 移除未加入的已知Member
          // TODO: cut the socket connection to the client
        }

        // TODO KAFKA-2720: only remove group in the background thread
        // 如果GroupMetadata中已经没有Member，就将GroupMetadata切换成Dead状态并从groupsCache中移除
        if (group.isEmpty) {
          // 切换成Dead状态
          group.transitionTo(Dead)
          // 从GroupMetadataManager的groupsCache集合中移除Group
          groupManager.removeGroup(group)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
        }
      }
      if (!group.is(Dead)) {
        // 递增generationId，选择该Consumer Group最终使用的PartitionAssignor
        group.initNextGeneration()
        info("Stabilized group %s generation %s".format(group.groupId, group.generationId))

        // trigger the awaiting join group response callback for all the members after rebalancing
        // 向GroupMetadata中所有的Member发送JoinGroupResponse响应
        for (member <- group.allMemberMetadata) {
          assert(member.awaitingJoinCallback != null)
          // 构造响应结果，Leader和Follower是不同的
          val joinResult = JoinGroupResult(
            // Group Leader会收到相应的分配结果
            members=if (member.memberId == group.leaderId) { group.currentMemberMetadata } else { Map.empty },
            memberId=member.memberId,
            generationId=group.generationId,
            subProtocol=group.protocol,
            leaderId=group.leaderId,
            errorCode=Errors.NONE.code)

          // 调用回调函数
          member.awaitingJoinCallback(joinResult)
          member.awaitingJoinCallback = null
          // 重置心跳延迟任务
          completeAndScheduleNextHeartbeatExpiration(group, member)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      /**
        * 四个条件满足一个即可：
        * 1. awaitingJoinCallback不为null，即消费者正在等待JoinGroupResponse
        * 2. awaitingSyncCallback不为null，即消费者正在等待SyncGroupResponse
        * 3. 最后一次收到心跳信息的时间与heartbeatDeadline的差距大于sessionTimeout
        * 4. 消费者已经离开了Consumer Group
        */
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  // DelayedHeartbeat到期时会执行该方法
  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        onMemberFailure(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  // 判断是否应该判定Member为存活状态
  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null || // awaitingJoinCallback不为null，即消费者正在等待JoinGroupResponse
      member.awaitingSyncCallback != null || // awaitingSyncCallback不为null，即消费者正在等待SyncGroupResponse
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline // 最后一次收到心跳信息的时间与heartbeatDeadline的差距大于sessionTimeout

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadingInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = OffsetConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}
