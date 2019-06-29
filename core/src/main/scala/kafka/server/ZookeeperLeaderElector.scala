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

import kafka.utils.ZkUtils._
import kafka.utils.CoreUtils._
import kafka.utils.{Json, SystemTime, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
  *
  * @param controllerContext 与之相关联的KafkaController的ControllerContext对象
  * @param electionPath 选举监听路径，即/controller
  * @param onBecomingLeader KafkaController成为Leader的回调方法，由与之相关联的KafkaController对象传入
  * @param onResigningAsLeader KafkaController从Leader成为Follower的回调方法，由与之相关联的KafkaController对象传入
  * @param brokerId 与之相关联的KafkaController所在的Broker的ID
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit,
                             brokerId: Int)
  extends LeaderElector with Logging {
  // 缓存当前的Controller LeaderId
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  // 当electionPath路径在Zookeeper中不存在时会尝试创建该路径
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  // 监听Zookeeper的/controller节点的数据变化，当此节点中保存的LeaderID发生变化时，出发LeaderChangeListener进行相应的处理
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      // 在Zookeeper的/controller节点上注册LeaderChangeListener进行监听
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      // 立即进行选举操作
      elect
    }
  }

  // 从Zookeeper的/controller路径读取KafkaController Leader的Broker ID
  private def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  // 具体的选举操作方法
  def elect: Boolean = {
    // 当前时间
    val timestamp = SystemTime.milliseconds.toString
    // 转换选举信息为JSON串
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

    // 获取Zookeeper中当前记录的Controller Leader的ID
    leaderId = getControllerID
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if(leaderId != -1) { // 1️⃣存在Controller Leader，放弃选举
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    try {
      // 尝试创建临时节点，如果临时节点已经存在，则抛出异常
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      // 走到这里说明临时节点创建成功
      // 更新LeaderId字段为当前Broker的ID，即当前Broker成功成为Controller Leader
      leaderId = brokerId

      // onBecomingLeader()实际调用了KafkaController的onControllerFailover()方法
      onBecomingLeader()
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        // 对onBecomingLeader()方法抛出的异常的处理，重置leaderId，并删除Zookeeper的/controller路径
        resign()
    }
    // 检测当前Broker是否是Controller Leader
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  // 处理onBecomingLeader()方法抛出的异常
  def resign() = {
    // 重置LeaderID为-1
    leaderId = -1
    // 删除Zookeeper中的/controller路径
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      inLock(controllerContext.controllerLock) {
        // 标识了发生变化前，对应的KafkaController的角色是否是Leader
        val amILeaderBeforeDataChange = amILeader
        // 记录新的Controller Leader的ID
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        // 如果当前Broker由Controller Leader变成Follower，则要进行清理工作
        if (amILeaderBeforeDataChange && !amILeader) // 数据变化前是Leader，变化后不是Leader，说明出现了角色切换
          // onResigningAsLeader()实际调用了KafkaController的onControllerResignation()
          onResigningAsLeader()
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
      * 当Zookeeper的/controller节点中的数据被删除时会触发handleDataDeleted()方法进行处理
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        if(amILeader) // 如果发生变化前对应的KafkaController的角色是Leader，则需要进行清理操作
          // onResigningAsLeader()实际是KafkaController的onControllerResignation()
          onResigningAsLeader()
        // 尝试新Controller Leader的选举
        elect
      }
    }
  }
}
