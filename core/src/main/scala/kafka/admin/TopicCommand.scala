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

package kafka.admin

import java.util.Properties
import joptsimple._
import kafka.common.{AdminCommandFailedException, Topic, TopicExistsException}
import kafka.consumer.{ConsumerConfig => OldConsumerConfig, Whitelist}
import kafka.coordinator.GroupCoordinator
import kafka.log.{Defaults, LogConfig}
import kafka.server.ConfigType
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
import scala.collection.JavaConversions._
import scala.collection._
import org.apache.kafka.clients.consumer.{ConsumerConfig => NewConsumerConfig}
import org.apache.kafka.common.internals.TopicConstants


object TopicCommand extends Logging {

  def main(args: Array[String]): Unit = {

    // 解析参数，TopicCommandOptions支持list、describe、create、alter和delete五种操作
    val opts = new TopicCommandOptions(args)

    // 检查参数长度
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Create, delete, describe, or change a topic.")

    // should have exactly one action
    // 获取对应的操作
    val actions = Seq(opts.createOpt, opts.listOpt, opts.alterOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")

    // 检查参数
    opts.checkArgs()

    // 获取Zookeeper连接
    val zkUtils = ZkUtils(opts.options.valueOf(opts.zkConnectOpt),
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())
    var exitCode = 0
    try {
      if(opts.options.has(opts.createOpt)) // --create参数，创建Topic
        createTopic(zkUtils, opts)
      else if(opts.options.has(opts.alterOpt)) // --alter参数，修改Topic
        alterTopic(zkUtils, opts)
      else if(opts.options.has(opts.listOpt)) // --list参数，列出Topic
        listTopics(zkUtils, opts)
      else if(opts.options.has(opts.describeOpt)) // --describe参数，查询Topic详细信息
        describeTopic(zkUtils, opts)
      else if(opts.options.has(opts.deleteOpt)) // --delete参数，删除Topic
        deleteTopic(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      zkUtils.close()
      System.exit(exitCode)
    }

  }

  // 从Zookeeper中获取Topic的列表，可以通过正则过滤
  private def getTopics(zkUtils: ZkUtils, opts: TopicCommandOptions): Seq[String] = {
    // 从从Zookeeper的/brokers/topics路径读取所有主题并按名排序
    val allTopics = zkUtils.getAllTopics().sorted
    // 判断是否有--topic参数，如果有可能需要通过正则匹配进行过滤
    if (opts.options.has(opts.topicOpt)) {
      val topicsSpec = opts.options.valueOf(opts.topicOpt)
      // 正则白名单
      val topicsFilter = new Whitelist(topicsSpec)
      // 进行过滤
      allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))
    } else
      allTopics
  }

  def createTopic(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    // 获取--topic参数
    val topic = opts.options.valueOf(opts.topicOpt)
    // 将--config参数解析成Properties对象
    val configs = parseTopicConfigsToBeAdded(opts)
    // 读取--if-not-exists参数
    val ifNotExists = if (opts.options.has(opts.ifNotExistsOpt)) true else false
    // 检测Topic名称中是否包含"."或"_"字符，若包含则输出警告信息
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")
    try {
      // 检测是否有--replica-assignment参数
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        /**
          * --replica-assignment参数的格式类似于：0:1:2,3:4:5,6:7:8，含义如下：
          * 1. 编号为0的分区分配在Broker-0、Broker-1和Broker-2上；
          * 2. 编号为1的分区分配在Broker-3、Broker-4和Broker-5上；
          * 3. 编号为2的分区分配在Broker-6、Broker-7和Broker-8上；
          *
          * 这里将--replica-assignment参数内容解析成Map[Int, Seq[Int]]格式，
          * 其key为分区的编号，value是其副本所分配的BrokerId
          */
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        // 检测max.message.bytes配置参数并给出提示
        warnOnMaxMessagesChange(configs, assignment.valuesIterator.next().length)
        // 对Topic名称和副本分配结果进行一系列的检测，并写入Zookeeper中
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignment, configs, update = false)
      } else {
        // 如果进行副本自动分配，必须指定--partitions和--replication-factor参数
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        // 获取--partitions参数的值
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        // 获取--replication-factor参数的值
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        // 检测max.message.bytes配置参数并给出提示
        warnOnMaxMessagesChange(configs, replicas)
        // 根据--disable-rack-aware参数决定分配副本时是否考虑机架信息
        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
                            else RackAwareMode.Enforced

        // 自动分配副本，并写入Zookeeper
        AdminUtils.createTopic(zkUtils, topic, partitions, replicas, configs, rackAwareMode)
      }
      println("Created topic \"%s\".".format(topic))
    } catch  {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }

  def alterTopic(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    // 从Zookeeper中获取与--topic参数正则匹配的Topic集合
    val topics = getTopics(zkUtils, opts)
    // 读取--if-exists参数
    val ifExists = if (opts.options.has(opts.ifExistsOpt)) true else false
    if (topics.length == 0 && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on ZK path %s".format(opts.options.valueOf(opts.topicOpt),
          opts.options.valueOf(opts.zkConnectOpt)))
    }
    topics.foreach { topic =>
      // 修改Topic配置项信息的功能路径为/config/topics/[topic_name]
      val configs = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
      // 判断是否包含--config配置或--delete-config配置
      if(opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {
        println("WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.")
        println("         Going forward, please use kafka-configs.sh for this functionality")

        // 得到新添加的和将删除的配置并进行更新
        val configsToBeAdded = parseTopicConfigsToBeAdded(opts)
        val configsToBeDeleted = parseTopicConfigsToBeDeleted(opts)
        // compile the final set of configs
        configs.putAll(configsToBeAdded)
        configsToBeDeleted.foreach(config => configs.remove(config))
        // 修改Zookeeper中的主题配置信息
        AdminUtils.changeTopicConfig(zkUtils, topic, configs)
        println("Updated config for topic \"%s\".".format(topic))
      }

      // 副本重新分配的功能
      // 检测是否包含--partitions参数
      if(opts.options.has(opts.partitionsOpt)) {
        // 不可修改__consumer_offsets主题，会抛出异常
        if (topic == TopicConstants.GROUP_METADATA_TOPIC_NAME) {
          throw new IllegalArgumentException("The number of partitions for the offsets topic cannot be changed.")
        }
        println("WARNING: If partitions are increased for a topic that has a key, the partition " +
          "logic or ordering of the messages will be affected")
        // 获取--partitions参数
        val nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue
        // 获取--replica-assignment参数
        val replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt)
        // 完成分区数量的增加以及副本分配
        AdminUtils.addPartitions(zkUtils, topic, nPartitions, replicaAssignmentStr)
        println("Adding partitions succeeded!")
      }
    }
  }

  def listTopics(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    // 从Zookeeper中获取Topic列表
    val topics = getTopics(zkUtils, opts)
    for(topic <- topics) {
      // 打印
      if (zkUtils.pathExists(getDeleteTopicPath(topic))) {
        // 如果标记为删除会打印" - marked for deletion"
        println("%s - marked for deletion".format(topic))
      } else {
        // 否则只打印主题名称
        println(topic)
      }
    }
  }

  def deleteTopic(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    // 获取指定的主题列表
    val topics = getTopics(zkUtils, opts)
    // --if-exists参数
    val ifExists = if (opts.options.has(opts.ifExistsOpt)) true else false

    // 根据参数进行检查
    if (topics.length == 0 && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on ZK path %s".format(opts.options.valueOf(opts.topicOpt),
          opts.options.valueOf(opts.zkConnectOpt)))
    }

    // 遍历指定的主题
    topics.foreach { topic =>
      try {
        if (Topic.isInternal(topic)) { // 不可删除内部分区
          throw new AdminOperationException("Topic %s is a kafka internal topic and is not allowed to be marked for deletion.".format(topic))
        } else {
          /**
            * 将待删除的Topic名称写入到Zookeeper的/admin/delete_topics路径下
            * 这将触发DeleteTopicsListener将待删除Topic交由TopicDeletionManager处理
            */
          zkUtils.createPersistentPath(getDeleteTopicPath(topic))
          println("Topic %s is marked for deletion.".format(topic))
          println("Note: This will have no impact if delete.topic.enable is not set to true.")
        }
      } catch {
        case e: ZkNodeExistsException =>
          println("Topic %s is already marked for deletion.".format(topic))
        case e: AdminOperationException =>
          throw e
        case e: Throwable =>
          throw new AdminOperationException("Error while deleting topic %s".format(topic))
      }
    }
  }

  def describeTopic(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    // 从Zookeeper读取指定Topic的列表
    val topics = getTopics(zkUtils, opts)

    // 获取--under-replicated-partitions参数
    val reportUnderReplicatedPartitions = if (opts.options.has(opts.reportUnderReplicatedPartitionsOpt)) true else false
    // 获取--unavailable-partitions参数
    val reportUnavailablePartitions = if (opts.options.has(opts.reportUnavailablePartitionsOpt)) true else false
    // 获取--topics-with-overrides参数
    val reportOverriddenConfigs = if (opts.options.has(opts.topicsWithOverridesOpt)) true else false
    // 获取所有可用的Broker
    val liveBrokers = zkUtils.getAllBrokersInCluster().map(_.id).toSet

    // 遍历需要查看的Topic
    for (topic <- topics) {
      // 从Zookeeper中获取每个Topic的分区分配
      zkUtils.getPartitionAssignmentForTopics(List(topic)).get(topic) match {
        case Some(topicPartitionAssignment) => // 能够获取到
          // 处理配置
          val describeConfigs: Boolean = !reportUnavailablePartitions && !reportUnderReplicatedPartitions
          val describePartitions: Boolean = !reportOverriddenConfigs

          // 对分区分配进行排序
          val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
          if (describeConfigs) {

            // 获取Topic的配置信息
            val configs = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)

            // 没有设置--topics-with-overrides参数，且--config配置为空
            if (!reportOverriddenConfigs || configs.size() != 0) {
              // 分区数
              val numPartitions = topicPartitionAssignment.size
              // 副本因子
              val replicationFactor = topicPartitionAssignment.head._2.size
              // 打印主题信息
              println("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s"
                .format(topic, numPartitions, replicationFactor, configs.map(kv => kv._1 + "=" + kv._2).mkString(",")))
            }
          }

          // 判断是否打印分区信息
          if (describePartitions) {
            // 遍历已排序的分区
            for ((partitionId, assignedReplicas) <- sortedPartitions) {
              // ISR副本集合
              val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partitionId)
              // Leader副本ID
              val leader = zkUtils.getLeaderForPartition(topic, partitionId)
              if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
                  (reportUnderReplicatedPartitions && inSyncReplicas.size < assignedReplicas.size) ||
                  (reportUnavailablePartitions && (!leader.isDefined || !liveBrokers.contains(leader.get)))) {
                // 打印信息
                print("\tTopic: " + topic)
                print("\tPartition: " + partitionId)
                print("\tLeader: " + (if(leader.isDefined) leader.get else "none"))
                print("\tReplicas: " + assignedReplicas.mkString(","))
                println("\tIsr: " + inSyncReplicas.mkString(","))
              }
            }
          }
        case None => // 主题不存在
          println("Topic " + topic + " doesn't exist!")
      }
    }
  }

  def parseTopicConfigsToBeAdded(opts: TopicCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
      "Invalid topic config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    LogConfig.validate(props)
    if (props.containsKey(LogConfig.MessageFormatVersionProp)) {
      println(s"WARNING: The configuration ${LogConfig.MessageFormatVersionProp}=${props.getProperty(LogConfig.MessageFormatVersionProp)} is specified. " +
      s"This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker.")
    }
    props
  }

  def parseTopicConfigsToBeDeleted(opts: TopicCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfigOpt)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfigOpt).map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      LogConfig.validateNames(propsToBeDeleted)
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      val duplicateBrokers = CoreUtils.duplicates(brokerList)
      if (duplicateBrokers.nonEmpty)
        throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicateBrokers.mkString(",")))
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }

  class TopicCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val listOpt = parser.accepts("list", "List all available topics.")
    val createOpt = parser.accepts("create", "Create a new topic.")
    val deleteOpt = parser.accepts("delete", "Delete a topic")
    val alterOpt = parser.accepts("alter", "Alter the number of partitions, replica assignment, and/or configuration for the topic.")
    val describeOpt = parser.accepts("describe", "List details for the given topics.")
    val helpOpt = parser.accepts("help", "Print usage information.")
    val topicOpt = parser.accepts("topic", "The topic to be create, alter or describe. Can also accept a regular " +
                                           "expression except for --create option")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val nl = System.getProperty("line.separator")
    val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered."  +
                                             "The following is a list of valid configurations: " + nl + LogConfig.configNames.map("\t" + _).mkString(nl) + nl +
                                             "See the Kafka documentation for full details on the topic configs.")
                           .withRequiredArg
                           .describedAs("name=value")
                           .ofType(classOf[String])
    val deleteConfigOpt = parser.accepts("delete-config", "A topic configuration override to be removed for an existing topic (see the list of configurations under the --config option).")
                           .withRequiredArg
                           .describedAs("name")
                           .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
      "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                           .withRequiredArg
                           .describedAs("# of partitions")
                           .ofType(classOf[java.lang.Integer])
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                           .withRequiredArg
                           .describedAs("replication factor")
                           .ofType(classOf[java.lang.Integer])
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                           .ofType(classOf[String])
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set when describing topics, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set when describing topics, only show partitions whose leader is not available")
    val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                                                "if set when describing topics, only show topics that have overridden configs")
    val ifExistsOpt = parser.accepts("if-exists",
                                     "if set when altering or deleting topics, the action will only execute if the topic exists")
    val ifNotExistsOpt = parser.accepts("if-not-exists",
                                        "if set when creating topics, the action will only execute if the topic does not already exist")

    val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")
    val options = parser.parse(args : _*)

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(alterOpt, createOpt, describeOpt, listOpt, deleteOpt)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      if (!options.has(listOpt) && !options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteConfigOpt, allTopicLevelOpts -- Set(alterOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, allTopicLevelOpts -- Set(createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, allTopicLevelOpts -- Set(createOpt,alterOpt))
      if(options.has(createOpt))
          CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, Set(partitionsOpt, replicationFactorOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnderReplicatedPartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnavailablePartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnavailablePartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicsWithOverridesOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + reportUnavailablePartitionsOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, ifExistsOpt, allTopicLevelOpts -- Set(alterOpt, deleteOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, allTopicLevelOpts -- Set(createOpt))
    }
  }
  def warnOnMaxMessagesChange(configs: Properties, replicas: Integer): Unit = {
    val maxMessageBytes =  configs.get(LogConfig.MaxMessageBytesProp) match {
      case n: String => n.toInt
      case _ => -1
    }
    if (maxMessageBytes > Defaults.MaxMessageSize)
      if (replicas > 1) {
        error(longMessageSizeWarning(maxMessageBytes))
        askToProceed
      }
      else
        warn(shortMessageSizeWarning(maxMessageBytes))
  }

  def askToProceed: Unit = {
    println("Are you sure you want to continue? [y/n]")
    if (!Console.readLine().equalsIgnoreCase("y")) {
      println("Ending your session")
      System.exit(0)
    }
  }

  def shortMessageSizeWarning(maxMessageBytes: Int): String = {
    "\n\n" +
      "*****************************************************************************************************\n" +
      "*** WARNING: you are creating a topic where the max.message.bytes is greater than the broker's    ***\n" +
      "*** default max.message.bytes. This operation is potentially dangerous. Consumers will get        ***\n" +
      s"*** failures if their fetch.message.max.bytes (old consumer) or ${NewConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG}         ***\n"+ 
      "*** (new consumer) < the value you are using.                                                     ***\n" +
      "*****************************************************************************************************\n" +
      s"- value set here: $maxMessageBytes\n" +
      s"- Default Old Consumer fetch.message.max.bytes: ${OldConsumerConfig.FetchSize}\n" +
      s"- Default New Consumer ${NewConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG}: ${NewConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES}\n" +
      s"- Default Broker max.message.bytes: ${kafka.server.Defaults.MessageMaxBytes}\n\n"
  }

  def longMessageSizeWarning(maxMessageBytes: Int): String = {
    "\n\n" +
      "*****************************************************************************************************\n" +
      "*** WARNING: you are creating a topic where the max.message.bytes is greater than the broker's    ***\n" +
      "*** default max.message.bytes. This operation is dangerous. There are two potential side effects: ***\n" +
      "*** - Consumers will get failures if their fetch.message.max.bytes (old consumer) or              ***\n" +
      s"***   ${NewConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG} (new consumer) < the value you are using                          ***\n" +
      "*** - Producer requests larger than replica.fetch.max.bytes will not replicate and hence have     ***\n" +
      "***   a higher risk of data loss                                                                  ***\n" +
      "*** You should ensure both of these settings are greater than the value set here before using     ***\n" +
      "*** this topic.                                                                                   ***\n" +
      "*****************************************************************************************************\n" +
      s"- value set here: $maxMessageBytes\n" +
      s"- Default Broker replica.fetch.max.bytes: ${kafka.server.Defaults.ReplicaFetchMaxBytes}\n" +
      s"- Default Broker max.message.bytes: ${kafka.server.Defaults.MessageMaxBytes}\n" +
      s"- Default Old Consumer fetch.message.max.bytes: ${OldConsumerConfig.FetchSize}\n" +
      s"- Default New Consumer ${NewConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG}: ${NewConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES}\n\n"
  }
}
