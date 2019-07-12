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

package kafka.tools

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.regex.{PatternSyntaxException, Pattern}
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Gauge
import joptsimple.OptionParser
import kafka.consumer.{BaseConsumerRecord, ConsumerIterator, BaseConsumer, Blacklist, ConsumerConfig, ConsumerThreadId, ConsumerTimeoutException, TopicFilter, Whitelist, ZookeeperConsumerConnector}
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.metrics.KafkaMetricsGroup
import kafka.serializer.DefaultDecoder
import kafka.utils.{CommandLineUtils, CoreUtils, Logging}
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, Consumer, ConsumerRecord, KafkaConsumer, CommitFailedException}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.util.control.ControlThrowable

/**
 * The mirror maker has the following architecture:
 * - There are N mirror maker thread shares one ZookeeperConsumerConnector and each owns a Kafka stream.
 * - All the mirror maker threads share one producer.
 * - Each mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss:
 *       1. use new producer with following settings
 *            acks=all
 *            retries=max integer
 *            max.block.ms=max long
 *            max.in.flight.requests.per.connection=1
 *       2. Consumer Settings
 *            auto.commit.enable=false
 *       3. Mirror Maker Setting:
 *            abort.on.send.failure=true
 */
object MirrorMaker extends Logging with KafkaMetricsGroup {
  // 生产者对象
  private var producer: MirrorMakerProducer = null
  // MirrorMakerThread线程集合
  private var mirrorMakerThreads: Seq[MirrorMakerThread] = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  // 记录MirrorMaker发送失败的消息个数
  private val numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  /**
    * 当MirrorMakerNewConsumer从源集群中将消息拉取到本地后，
    * 会先经过MirrorMakerMessageHandler处理，再发送给目的集群。
    * 此字段的具体类型是由message.handler参数指定的，默认值是defaultMirrorMakerMessageHandler。
    */
  private var messageHandler: MirrorMakerMessageHandler = null
  // 指定生产者进行flush()操作和提交offset的周期
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  def main(args: Array[String]) {

    info("Starting mirror maker")
    try {
      // 解析命令行参数
      val parser = new OptionParser

      val consumerConfigOpt = parser.accepts("consumer.config",
        "Embedded consumer config for consuming from the source cluster.")
        .withRequiredArg()
        .describedAs("config file")
        .ofType(classOf[String])

      val useNewConsumerOpt = parser.accepts("new.consumer",
        "Use new consumer in mirror maker.")

      val producerConfigOpt = parser.accepts("producer.config",
        "Embedded producer config.")
        .withRequiredArg()
        .describedAs("config file")
        .ofType(classOf[String])

      val numStreamsOpt = parser.accepts("num.streams",
        "Number of consumption streams.")
        .withRequiredArg()
        .describedAs("Number of threads")
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(1)

      val whitelistOpt = parser.accepts("whitelist",
        "Whitelist of topics to mirror.")
        .withRequiredArg()
        .describedAs("Java regex (String)")
        .ofType(classOf[String])

      val blacklistOpt = parser.accepts("blacklist",
        "Blacklist of topics to mirror. Only old consumer supports blacklist.")
        .withRequiredArg()
        .describedAs("Java regex (String)")
        .ofType(classOf[String])

      val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
        "Offset commit interval in ms")
        .withRequiredArg()
        .describedAs("offset commit interval in millisecond")
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(60000)

      val consumerRebalanceListenerOpt = parser.accepts("consumer.rebalance.listener",
        "The consumer rebalance listener to use for mirror maker consumer.")
        .withRequiredArg()
        .describedAs("A custom rebalance listener of type ConsumerRebalanceListener")
        .ofType(classOf[String])

      val rebalanceListenerArgsOpt = parser.accepts("rebalance.listener.args",
        "Arguments used by custom rebalance listener for mirror maker consumer")
        .withRequiredArg()
        .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
        .ofType(classOf[String])

      val messageHandlerOpt = parser.accepts("message.handler",
        "Message handler which will process every record in-between consumer and producer.")
        .withRequiredArg()
        .describedAs("A custom message handler of type MirrorMakerMessageHandler")
        .ofType(classOf[String])

      val messageHandlerArgsOpt = parser.accepts("message.handler.args",
        "Arguments used by custom message handler for mirror maker.")
        .withRequiredArg()
        .describedAs("Arguments passed to message handler constructor.")
        .ofType(classOf[String])

      val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
        "Configure the mirror maker to exit on a failed send.")
        .withRequiredArg()
        .describedAs("Stop the entire mirror maker when a send failure occurs")
        .ofType(classOf[String])
        .defaultsTo("true")

      val helpOpt = parser.accepts("help", "Print this message.")

      if (args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")


      val options = parser.parse(args: _*)

      if (options.has(helpOpt)) {
        parser.printHelpOn(System.out)
        System.exit(0)
      }

      CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)

      // 是否使用--new.consumer参数指定使用新消费者
      val useNewConsumer = options.has(useNewConsumerOpt)
      if (useNewConsumer) {
        if (options.has(blacklistOpt)) { // --blacklist
          error("blacklist can not be used when using new consumer in mirror maker. Use whitelist instead.")
          System.exit(1)
        }
        if (!options.has(whitelistOpt)) { // --whitelist
          error("whitelist must be specified when using new consumer in mirror maker.")
          System.exit(1)
        }
      } else {
        if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
          error("Exactly one of whitelist or blacklist is required.")
          System.exit(1)
        }
      }

      // 是否在发送失败时停止发送，abort.on.send.failure
      abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
      // offset提交周期，offset.commit.interval.ms
      offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()
      // 流数量，num.streams
      val numStreams = options.valueOf(numStreamsOpt).intValue()

      // 设置JVM关闭钩子
      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run() {
          cleanShutdown()
        }
      })

      // create producer
      // 获取生产者相关的配置
      val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
      // Defaults to no data loss settings.
      maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
      // Always set producer key and value serializer to ByteArraySerializer.
      producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
      producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

      // 创建MirrorMakerProducer生产者
      producer = new MirrorMakerProducer(producerProps)

      // Create consumers
      // 创建消费者
      val mirrorMakerConsumers = if (!useNewConsumer) {
        val customRebalanceListener = {
          // 根据consumer.rebalance.listener参数决定是否创建ConsumerRebalanceListener
          val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
          if (customRebalanceListenerClass != null) {
            val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
            if (rebalanceListenerArgs != null) {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
            } else {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
            }
          } else {
            None
          }
        }

        // 检测customRebalanceListener的类型是否正确，若不正确则抛出异常
        if (customRebalanceListener.exists(!_.isInstanceOf[ConsumerRebalanceListener]))
          throw new IllegalArgumentException("The rebalance listener should be an instance of kafka.consumer.ConsumerRebalanceListener")

        // 创建旧消费者
        createOldConsumers(
          numStreams, // 消费者个数
          options.valueOf(consumerConfigOpt), //消费者的配置
          customRebalanceListener,
          Option(options.valueOf(whitelistOpt)), // 白名单
          Option(options.valueOf(blacklistOpt))) // 黑名单
      } else {
        // 根据consumer.rebalance.listener参数决定是否创建ConsumerRebalanceListener
        val customRebalanceListener = {
          val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
          if (customRebalanceListenerClass != null) {
            val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
            if (rebalanceListenerArgs != null) {
                Some(CoreUtils.createObject[org.apache.kafka.clients.consumer.ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
            } else {
                Some(CoreUtils.createObject[org.apache.kafka.clients.consumer.ConsumerRebalanceListener](customRebalanceListenerClass))
            }
          } else {
            None
          }
        }

        // 检测customRebalanceListener的类型是否正确，若不正确则抛出异常
        if (customRebalanceListener.exists(!_.isInstanceOf[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]))
          throw new IllegalArgumentException("The rebalance listener should be an instance of" +
            "org.apache.kafka.clients.consumer.ConsumerRebalanceListner")

        // 创建新消费者
        createNewConsumers(
          numStreams, // 消费者个数
          options.valueOf(consumerConfigOpt), // 消费者相关配置
          customRebalanceListener,
          Option(options.valueOf(whitelistOpt))) // 白名单
      }

      // Create mirror maker threads.
      // 为每个消费者创建一个MirrorMakerThread线程
      mirrorMakerThreads = (0 until numStreams) map (i =>
        new MirrorMakerThread(mirrorMakerConsumers(i), i))

      // Create and initialize message handler
      // 根据message.handler参数创建并初始化MessageHandler
      val customMessageHandlerClass = options.valueOf(messageHandlerOpt) // message.handler
      val messageHandlerArgs = options.valueOf(messageHandlerArgsOpt) // message.handler.args
      // 创建MessageHandler
      messageHandler = {
        if (customMessageHandlerClass != null) {
          if (messageHandlerArgs != null)
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass, messageHandlerArgs)
          else
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass)
        } else {
          defaultMirrorMakerMessageHandler
        }
      }
    } catch {
      case ct : ControlThrowable => throw ct
      case t : Throwable =>
        error("Exception when starting mirror maker.", t)
    }

    // 启动MirrorMakerThread线程
    mirrorMakerThreads.foreach(_.start())
    // 主线程阻塞，等待MirrorMakerThread线程全部结束
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  private def createOldConsumers(numStreams: Int,
                                consumerConfigPath: String,
                                customRebalanceListener: Option[ConsumerRebalanceListener],
                                whitelist: Option[String],
                                blacklist: Option[String]) : Seq[MirrorMakerBaseConsumer] = {
    // Create consumer connector
    val consumerConfigProps = Utils.loadProps(consumerConfigPath)
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    val connectors = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      val consumerConfig = new ConsumerConfig(consumerConfigProps)
      new ZookeeperConsumerConnector(consumerConfig)
    }

    // create filters
    val filterSpec = if (whitelist.isDefined)
      new Whitelist(whitelist.get)
    else if (blacklist.isDefined)
      new Blacklist(blacklist.get)
    else
      throw new IllegalArgumentException("Either whitelist or blacklist should be defined!")
    (0 until numStreams) map { i =>
      val consumer = new MirrorMakerOldConsumer(connectors(i), filterSpec)
      val consumerRebalanceListener = new InternalRebalanceListenerForOldConsumer(consumer, customRebalanceListener)
      connectors(i).setConsumerRebalanceListener(consumerRebalanceListener)
      consumer
    }
  }

  def createNewConsumers(numStreams: Int,
                         consumerConfigPath: String,
                         customRebalanceListener: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener],
                         whitelist: Option[String]) : Seq[MirrorMakerBaseConsumer] = {
    // Create consumer connector
    // 获取KafkaConsumer的相关配置项
    val consumerConfigProps = Utils.loadProps(consumerConfigPath)
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "enable.auto.commit", "false")
    // Hardcode the deserializer to ByteArrayDeserializer
    consumerConfigProps.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
    consumerConfigProps.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    // 创建指定数量的KafkaConsumer对象
    val consumers = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfigProps)
    }
    whitelist.getOrElse(throw new IllegalArgumentException("White list cannot be empty for new consumer"))
    // 创建MirrorMakerNewConsumer
    consumers.map(consumer => new MirrorMakerNewConsumer(consumer, customRebalanceListener, whitelist))
  }

  def commitOffsets(mirrorMakerConsumer: MirrorMakerBaseConsumer) {
    if (!exitingOnSendFailure) {
      trace("Committing offsets.")
      try {
        mirrorMakerConsumer.commit()
      } catch {
        case e: WakeupException =>
          // we only call wakeup() once to close the consumer,
          // so if we catch it in commit we can safely retry
          // and re-throw to break the loop
          mirrorMakerConsumer.commit()
          throw e

        case e: CommitFailedException =>
          warn("Failed to commit offsets because the consumer group has rebalanced and assigned partitions to " +
            "another instance. If you see this regularly, it could indicate that you need to either increase " +
            s"the consumer's ${consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG} or reduce the number of records " +
            s"handled on each iteration with ${consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG}")
      }
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown() {
    if (isShuttingdown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (mirrorMakerThreads != null) {
        mirrorMakerThreads.foreach(_.shutdown())
        mirrorMakerThreads.foreach(_.awaitShutdown())
      }
      info("Closing producer.")
      producer.close()
      info("Kafka mirror maker shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String) {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run() {
      info("Starting mirror maker thread " + threadName)
      try {
        // 初始化MirrorMakerNewConsumer
        mirrorMakerConsumer.init()

        // We need the two while loop to make sure when old consumer is used, even there is no message we
        // still commit offset. When new consumer is used, this is handled by poll(timeout).
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            // 检测exitingOnSendFailure、shuttingDown等标记
            while (!exitingOnSendFailure && !shuttingDown && mirrorMakerConsumer.hasData) {
              // 从源集群中获取消息
              val data = mirrorMakerConsumer.receive()
              trace("Sending message with value size %d and offset %d".format(data.value.length, data.offset))
              // 通过MirrorMakerMessageHandler创建ProducerRecord
              val records = messageHandler.handle(data)
              // 发送消息
              records.foreach(producer.send)
              // 尝试提交offset
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case cte: ConsumerTimeoutException =>
              trace("Caught ConsumerTimeoutException, continue iteration.")
            case we: WakeupException =>
              trace("Caught ConsumerWakeupException, continue iteration.")
          }
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        CoreUtils.swallow {
          info("Flushing producer.")
          producer.flush()

          // note that this commit is skipped if flush() fails which ensures that we don't lose messages
          info("Committing consumer offsets.")
          commitOffsets(mirrorMakerConsumer)
        }

        info("Shutting down consumer connectors.")
        CoreUtils.swallow(mirrorMakerConsumer.stop())
        CoreUtils.swallow(mirrorMakerConsumer.cleanup())
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingdown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets() {
      // 满足offset.commit.interval.ms参数指定的时间间隔，才提交一次offset
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        debug("Committing MirrorMaker state automatically.")
        // 将KafkaProducer缓冲的消息发送出去
        producer.flush()
        // 提交MirrorMakerNewConsumer的offsets字段中记录的offset
        commitOffsets(mirrorMakerConsumer)
        // 记录最近一次提交时间戳
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown() {
      try {
        info(threadName + " shutting down")
        shuttingDown = true
        mirrorMakerConsumer.stop()
      }
      catch {
        case ie: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case ie: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  private[kafka] trait MirrorMakerBaseConsumer extends BaseConsumer {
    def init()
    def hasData : Boolean
  }

  private class MirrorMakerOldConsumer(connector: ZookeeperConsumerConnector,
                                       filterSpec: TopicFilter) extends MirrorMakerBaseConsumer {
    private var iter: ConsumerIterator[Array[Byte], Array[Byte]] = null

    override def init() {
      // Creating one stream per each connector instance
      val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())
      require(streams.size == 1)
      val stream = streams(0)
      iter = stream.iterator()
    }

    override def hasData = iter.hasNext()

    override def receive() : BaseConsumerRecord = {
      val messageAndMetadata = iter.next()
      BaseConsumerRecord(messageAndMetadata.topic,
                         messageAndMetadata.partition,
                         messageAndMetadata.offset,
                         messageAndMetadata.timestamp,
                         messageAndMetadata.timestampType,
                         messageAndMetadata.key,
                         messageAndMetadata.message)
    }

    override def stop() {
      // Do nothing
    }

    override def cleanup() {
      connector.shutdown()
    }

    override def commit() {
      connector.commitOffsets
    }
  }

  private class MirrorMakerNewConsumer(consumer: Consumer[Array[Byte], Array[Byte]],
                                       customRebalanceListener: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener],
                                       whitelistOpt: Option[String])
    extends MirrorMakerBaseConsumer {
    val regex = whitelistOpt.getOrElse(throw new IllegalArgumentException("New consumer only supports whitelist."))
    var recordIter: java.util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null

    // TODO: we need to manually maintain the consumed offsets for new consumer
    // since its internal consumed position is updated in batch rather than one
    // record at a time, this can be resolved when we break the unification of both consumers
    // 使用HashMap维护自己消费的offset
    private val offsets = new HashMap[TopicPartition, Long]()

    override def init() {
      debug("Initiating new consumer")
      // 创建InternalRebalanceListenerForNewConsumer
      val consumerRebalanceListener = new InternalRebalanceListenerForNewConsumer(this, customRebalanceListener)
      if (whitelistOpt.isDefined) {
        try {
          // 订阅指定白名单
          consumer.subscribe(Pattern.compile(whitelistOpt.get), consumerRebalanceListener)
        } catch {
          case pse: PatternSyntaxException =>
            error("Invalid expression syntax: %s".format(whitelistOpt.get))
            throw pse
        }
      }
    }

    override def hasData = true

    override def receive() : BaseConsumerRecord = {
      if (recordIter == null || !recordIter.hasNext) {
        // 从源集群中拉取消息
        recordIter = consumer.poll(1000).iterator
        if (!recordIter.hasNext)
          throw new ConsumerTimeoutException
      }

      val record = recordIter.next()
      val tp = new TopicPartition(record.topic, record.partition)

      // 记录消费的offset
      offsets.put(tp, record.offset + 1)

      // 返回拉取的消息
      BaseConsumerRecord(record.topic,
                         record.partition,
                         record.offset,
                         record.timestamp,
                         record.timestampType,
                         record.key,
                         record.value)
    }

    override def stop() {
      consumer.wakeup()
    }

    override def cleanup() {
      consumer.close()
    }

    override def commit() {
      consumer.commitSync(offsets.map { case (tp, offset) =>  (tp, new OffsetAndMetadata(offset, ""))})
      offsets.clear()
    }
  }

  private class InternalRebalanceListenerForNewConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForNewConsumer: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener])
    extends org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
      // 将KafkaProducer缓存的消息全部发送到目标集群
      producer.flush()
      // 提交offsets集合中记录的offset
      commitOffsets(mirrorMakerConsumer)
      // 调用consumer.rebalance.listener参数指定的ConsumerRebalanceListener实现自定义的功能
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsRevoked(partitions))
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsAssigned(partitions))
    }
  }

  private class InternalRebalanceListenerForOldConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForOldConsumer: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]) {
      producer.flush()
      commitOffsets(mirrorMakerConsumer)
      // invoke custom consumer rebalance listener
      customRebalanceListenerForOldConsumer.foreach(_.beforeReleasingPartitions(partitionOwnership))
    }

    override def beforeStartingFetchers(consumerId: String,
                                        partitionAssignment: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]]) {
      customRebalanceListenerForOldConsumer.foreach(_.beforeStartingFetchers(consumerId, partitionAssignment))
    }
  }

  private class MirrorMakerProducer(val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]) {
      if (sync) { // 同步发送
        this.producer.send(record).get()
      } else { // 异步发送
          this.producer.send(record,
            // 发送完成的回调
            new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush() {
      this.producer.flush()
    }

    def close() {
      this.producer.close()
    }

    def close(timeout: Long) {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
    }
  }

  /**
    * 统计发送失败的消息数量，
    * 根据abort.on.send.failure配置决定在出现发送失败的情况时是否关闭生产者
    */
  private class MirrorMakerProducerCallback (topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null) { // 出现异常的相关处理
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        // 通过父类的实现输出错误日志
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        /** 如果设置了abort.on.send.failure参数，则停止MirrorMaker，
          * 否则忽略异常，继续发送后面的消息
          */
        if (abortOnSendFailure) {
          info("Closing producer due to send failure.")
          // 设置为true后会通知全部MirrorMakerThread停止
          exitingOnSendFailure = true
          producer.close(0)
        }
        // 记录发送失败的消息数量
        numDroppedMessages.incrementAndGet()
      }
    }
  }

  /**
   * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
   */
  trait MirrorMakerMessageHandler {
    def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private[tools] object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](record.topic, null, record.timestamp, record.key, record.value))
    }
  }

}
