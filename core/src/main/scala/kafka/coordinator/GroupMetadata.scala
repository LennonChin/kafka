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

import kafka.utils.nonthreadsafe

import java.util.UUID

import org.apache.kafka.common.protocol.Errors

import collection.mutable

private[coordinator] sealed trait GroupState { def state: Byte }

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => AwaitingSync
 *             all members have left the group => Dead
 */
private[coordinator] case object PreparingRebalance extends GroupState { val state: Byte = 1 }

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 */
private[coordinator] case object AwaitingSync extends GroupState { val state: Byte = 5}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 */
private[coordinator] case object Stable extends GroupState { val state: Byte = 3 }

/**
 * Group has no more members
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[coordinator] case object Dead extends GroupState { val state: Byte = 4 }


private object GroupMetadata {
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, AwaitingSync),
      AwaitingSync -> Set(PreparingRebalance),
      Stable -> Set(AwaitingSync),
      PreparingRebalance -> Set(Stable, AwaitingSync))
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
  *
  *  记录了Consumer Group的元数据
  *
  * @param groupId 对应的Consumer Group的ID
  * @param protocolType
  */
@nonthreadsafe
private[coordinator] class GroupMetadata(val groupId: String, val protocolType: String) {

  // key是memberId，value为对应的MemberMetadata对象
  private val members = new mutable.HashMap[String, MemberMetadata]
  // 标识当前Consumer Group所处的状态
  private var state: GroupState = Stable
  // 标识当前Consumer Group的年代信息，避免受到过期请求的影响
  var generationId = 0
  // 记录Consumer Group中的Leader消费者的memberId
  var leaderId: String = null
  // 记录当前Consumer Group选择的PartitionAssignor
  var protocol: String = null

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)

  def add(memberId: String, member: MemberMetadata) {
    assert(supportsProtocols(member.protocols))

    if (leaderId == null)
      // 将第一个加入的Member作为Group Leader
      leaderId = memberId
    members.put(memberId, member)
  }

  def remove(memberId: String) {
    members.remove(memberId)
    if (memberId == leaderId) { // 如果移除的是Group Leader
      // 重新选择Group Leader
      leaderId = if (members.isEmpty) {
        null
      } else {
        // Group Leader被删除，则重新选择第一个Member作为Group Leader
        members.keys.head
      }
    }
  }

  def currentState = state

  // Member集合是否为空
  def isEmpty = members.isEmpty

  def notYetRejoinedMembers = members.values.filter(_.awaitingJoinCallback == null).toList

  def allMembers = members.keySet

  // 所有Member对应的MemberMetadata的集合
  def allMemberMetadata = members.values.toList

  def rebalanceTimeout = members.values.foldLeft(0) {(timeout, member) =>
    timeout.max(member.sessionTimeoutMs)
  }

  // TODO: decide if ids should be predictable or random
  def generateMemberIdSuffix = UUID.randomUUID().toString

  def canRebalance = state == Stable || state == AwaitingSync

  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
  }

  // 为Consumer Group选择合适的PartitionAssignor
  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    // 所有Member都支持的协议作为"候选协议"集合
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    /**
      * 每个Member都会通过vote()方法进行投票，
      * 每个Member会为其支持的协议中的第一个"候选协议"投一票，
      * 最终将选择得票最多的PartitionAssignor
      */
    val votes: List[(String, Int)] = allMemberMetadata // 先获取所有Member对应的MemberMetadata
      .map(_.vote(candidates)) // 使用MemberMetadata的vote()方法进行投票
      .groupBy(identity) // 分组
      .mapValues(_.size) // 计算每种PartitionAssignor的票数
      .toList

    // 取得票最多的PartitionAssignor
    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    allMemberMetadata
      .map(_.protocols) // 所有Member支持的协议集合
      .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols)
  }

  def supportsProtocols(memberProtocols: Set[String]) = {
    isEmpty || (memberProtocols & candidateProtocols).nonEmpty
  }

  def initNextGeneration() = {
    assert(notYetRejoinedMembers == List.empty[MemberMetadata])
    generationId += 1
    protocol = selectProtocol
    transitionTo(AwaitingSync)
  }

  def currentMemberMetadata: Map[String, Array[Byte]] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol))}.toMap
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val members = this.members.values.map{ member => member.summary(protocol) }.toList
      GroupSummary(state.toString, protocolType, protocol, members)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }.toList
      GroupSummary(state.toString, protocolType, GroupCoordinator.NoProtocol, members)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType)
  }

  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override def toString = {
    "[%s,%s,%s,%s]".format(groupId, protocolType, currentState.toString, members)
  }
}