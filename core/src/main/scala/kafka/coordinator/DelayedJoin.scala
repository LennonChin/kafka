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

import kafka.server.DelayedOperation

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 *
  * 等待Consumer Group中所有的消费者发送JoinGroupRequest申请加入。
  * 每当处理完新收到的JoinGroupRequest时，都会检测相关的DelayedJoin是否能够完成，
  * 经过一段时间的等待，DelayedJoin也会到期执行。
  *
  * @param coordinator GroupCoordinator对象
  * @param group DelayedJoin对应的GroupMetadata对象
  * @param sessionTimeout 到期时长，是GroupMetadata中所有Member设置的超时时间的最大值
  */
private[coordinator] class DelayedJoin(coordinator: GroupCoordinator,
                                            group: GroupMetadata,
                                            sessionTimeout: Long)
  extends DelayedOperation(sessionTimeout) {

  // 三个方法调用的都是GroupCoordinator的方法
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete)
  override def onExpiration() = coordinator.onExpireJoin()
  // 当已知Member都已申请重新加入或DelayedJoin到期时执行该方法
  override def onComplete() = coordinator.onCompleteJoin(group)
}
