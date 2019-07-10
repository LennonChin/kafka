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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  // 服务器可以有多块网卡，Kafka可以配置监听多个端口，Endpoint类封装了需要监听的host、port及网络协议
  private val endpoints = config.listeners
  // Processor的线程个数
  private val numProcessorThreads = config.numNetworkThreads
  // 在RequestChannel的requestQueue中缓存的最大请求个数
  private val maxQueuedRequests = config.queuedMaxRequests
  // Processor线程的总个数
  private val totalProcessorThreads = numProcessorThreads * endpoints.size
  // 每个IP上能创建的最大连接数
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  // Map[String,Int]类型，具体指定某IP上最大的连接数，这里指定的最大连接数会覆盖上面maxConnectionsPerIp字段的值。
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "
  // 创建Processor线程与Handler线程之间交换数据的队列，其中有totalProcessorThreads个responseQueue队列
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  // 创建保存Processor线程的数组。此数组中包含所有Endpoint对应的Processors线程
  private val processors = new Array[Processor](totalProcessorThreads)
  // 创建Acceptor对象集合，每个Endpoint对应一个Acceptor对象
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  /**
    * 在ConnectionQuotas中，提供了控制每个IP上的最大连接数的功能。
    * 底层通过一个Map对象，记录每个IP地址上建立的连接数，
    * 创建新Connect时与maxConnectionsPerIpOverrides指定的最大值（或maxConnectionsPerIp）进行比较，若超出限制，则报错。
    * 因为有多个Acceptor线程并发访问底层的Map对象，则需要synchronized进行同步。
    */
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
    * 初始化SocketServer的核心代码
   */
  def startup() {
    // 同步代码块
    this.synchronized {
      // 创建ConnectionQuotas
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      // Socket的sendBuffer大小（socket.send.buffer.bytes）
      val sendBufferSize = config.socketSendBufferBytes
      // Socket的receiveBuffer大小（socket.receive.buffer.bytes）
      val recvBufferSize = config.socketReceiveBufferBytes
      // broker的ID（broker.id）
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      // 遍历Endpoints集合
      endpoints.values.foreach { endpoint =>
        val protocol = endpoint.protocolType
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        // 循环 processorBeginIndex ~ processorEndIndex次
        for (i <- processorBeginIndex until processorEndIndex)
          // 创建Processor对象，放入processors数组
          processors(i) = newProcessor(i, connectionQuotas, protocol)

        // 创建Acceptor，同时为processor创建对应的线程
        val acceptor = new Acceptor(endpoint,
          sendBufferSize,
          recvBufferSize,
          brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), // processors数组中与此Acceptor对象对应的Processor对象
          connectionQuotas)
        // 将endpoint与acceptor放入acceptors字典
        acceptors.put(endpoint, acceptor)
        // 创建并启动Acceptor对应的线程
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        // 主线程阻塞等待Acceptor线程启动完成
        acceptor.awaitStartup()
        // 修改processorBeginIndex
        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  /**
    * 向RequestChannel中添加监听器，RequestChannel是Processor与Handler传递数据的桥梁
    * 当Handler线程向某个responseQueue中写入数据时，会唤醒对应的Processor线程进行处理
    */
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    // 同步
    this.synchronized {
      // 调用所有Acceptor的shutdown
      acceptors.values.foreach(_.shutdown)
      // 调用所有Processor的shutdown
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
  * 实现了Runnable接口
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {
  // CountDownLatch对象，count为1，标识当前线程的startup操作是否完成
  private val startupLatch = new CountDownLatch(1)
  // CountDownLatch对象，count为1，标识当前线程的shutdown操作是否完成
  private val shutdownLatch = new CountDownLatch(1)
  // 标识当前线程是否存活
  private val alive = new AtomicBoolean(true)

  // 由子类实现
  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
    * 阻塞等待关闭
   */
  def shutdown(): Unit = {
    // 修改运行状态
    alive.set(false)
    // 唤醒当前的AbstractServerThread线程
    wakeup()
    // 调用startupLatch的await，阻塞等待关闭
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
    * 调用startupLatch的await，阻塞等待启动
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
    * 启动完成，唤醒阻塞的线程
   */
  protected def startupComplete() = {
    // 调用startupLatch的countDown，唤醒阻塞线程
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
    * 调用shutdownLatch的countDown，唤醒阻塞线程
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
    * 关闭指定连接
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        // 减少connectionQuotas中记录的连接数
        connectionQuotas.dec(address)
      // 根据传入的connectionId关闭SocketChannel
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
  * 接收客户端建立连接的请求，创建Socket连接并分配给Processor处理
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  // 创建Java NIO Selector
  private val nioSelector = NSelector.open()
  // 创建ServerSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  // 同步处理
  this.synchronized {
    // 遍历processors
    processors.foreach { processor =>
      // 为对应的Processor创建线程并启动
      Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    // 注册OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    // 标识当前线程已启动完成
    startupComplete()
    try {
      var currentProcessor = 0
      // 当线程在运行时
      while (isRunning) {
        try {
          // select操作，超时500ms
          val ready = nioSelector.select(500)
          // 有select到的SelectionKey
          if (ready > 0) {
            // 获取所有SelectionKey
            val keys = nioSelector.selectedKeys()
            // 遍历所有SelectionKey
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                // 移除
                iter.remove()
                if (key.isAcceptable)
                  // 如果SelectionKey是Acceptable的，将这个SelectionKey交给一个Processor线程
                  accept(key, processors(currentProcessor))
                else
                  // 否则抛出IllegalStateException异常
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // Round Robin方式从processors数组中选择Processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e // ControlThrowable异常就抛出
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      // 关掉ServerSocketChannel和NIO Selector，并吞掉异常
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      // 标识关闭操作完成
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    // 获取ServerSocketChannel
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 接受连接
    val socketChannel = serverSocketChannel.accept()
    try {
      // 增加ConnectQuotas中记录的连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)

      // 配置非阻塞、TCP NoDelay、KeepAlive、SendBufferSize
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      // 将socketChannel交给processor处理
      processor.accept(socketChannel)
    } catch {
      // 异常处理
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        // 出现异常会关闭SocketChannel
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
  * 主要完成读取请求和写会响应等操作，但不参与具体业务逻辑的处理
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel, // Processor与Handler线程之间传递数据的队列
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  // 保存了由此Processor处理的新建的SocketChannel
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  // 保存未发送的响应，会在发送成功后移除
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  // 负责管理网络连接
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

  override def run() {
    // 标识启动完成，唤醒等待Processor线程完成的线程
    startupComplete()
    while (isRunning) { // isRunning字段取的是alive.get，因此在Processor调用shutdown()时会将alive置为false，循环会结束
      try {
        // setup any new connections that have been queued up
        // 处理每个SocketChannel注册OP_READ事件的工作
        configureNewConnections()
        // register any new responses for writing
        // 处理responseQueue队列中缓存的Response
        processNewResponses()
        // 调用poll()方法读取请求，发送响应
        poll()
        // 处理KSelector.completedReceives队列
        processCompletedReceives()
        // 处理KSelector.completedSends队列
        processCompletedSends()
        // 处理KSelector.disconnected队列
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {
    // 从requestChannel中获取对应的responseQueue队列，并得到出队的RequestChannel.Response对象
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        // 匹配响应动作类型
        curr.responseAction match {
          // 无操作
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            // unmute，其实是注册OP_READ事件
            selector.unmute(curr.request.connectionId)
          case RequestChannel.SendAction =>
            // 需要发送响应给客户端，会将响应放入inflightResponses队列缓存
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            // 需要关闭连接
            close(selector, curr.request.connectionId)
        }
      } finally {
        // 继续处理responseQueue
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /**
    * `protected` for test usage
    * 将连接放入inflightResponses队列，等待发送
    * */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    // 获取对应的KafkaChannel
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    // 如果KafkaChannel为null，表示连接可能失效了，记录错误信息，并更新到监控系统
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      // 将响应绑定到KSelector的send属性上
      selector.send(response.responseSend)
      // 添加响应到inflightResponses字典
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    /**
      * 将读取的请求、发送成功的请求以及断开的连接放入其
      * completedReceives、completedSends、disconnected队列中等待处理
      */
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  /**
    * 遍历completedReceives，将NetworkReceive、ProcessorId、身份认证信息一起封装成RequestChannel.Request对象
    * 并放入RequestChannel.requestQueue队列中，等待Handler线程的后续处理。
    * 之后，取消对应KafkaChannel注册的OP_READ事件，表示在发送响应之前，此连接不能再读取任何请求了。
    */
  private def processCompletedReceives() {
    // 遍历KSelector.completedReceives队列
    selector.completedReceives.asScala.foreach { receive =>
      try {
        // 获取对应的KafkaChannel
        val channel = selector.channel(receive.source)
        // 从KafkaChannel中封装的SaslServerAuthenticator对象中获取authorizationID信息，并封装成Session，用于权限控制
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)
        // 将Session与请求数据封装成RequestChannel.Request对象
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
        // 将RequestChannel.Requestd放入RequestChannel.requestQueue队列中等待Handler线程的后续处理
        requestChannel.sendRequest(req)
        // 取消注册的OP_READ事件，连接将不再读取数据
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    // 遍历completedSends队列
    selector.completedSends.asScala.foreach { send =>
      // 从inflightResponses字典中取出Response
      val resp = inflightResponses.remove(send.destination).getOrElse {
        // 如果没取到就抛出异常
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      // 调用Request的updateRequestMetrics()更新度量值
      resp.request.updateRequestMetrics()
      // 注册OP_READ事件，允许连接继续读取数据
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    // 遍历disconnected队列
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      // 从inflightResponses中移除该连接对应的所有Response
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      // 减少ConnectionQuotas中记录的连接数，为后续的新建连接做准备
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    // 将SocketChannel添加到newConnections队列中
    newConnections.add(socketChannel)
    // 唤醒底层的Java NIO Selector的wakeup()
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    // 只有在newConnections不为空时才进行
    while (!newConnections.isEmpty) {
      // poll一个SocketChannel对象
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        // 根据本地Host、port，客户端Host、port构造ConnectionId对象
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 注册OP_READ事件，这里使用的是Kafka封装的KSelector
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    // 关闭KSelector上所有的KafkaChannel，并关闭KSelector
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup() // 调用Java NIO Selector的wakeup()

}

/**
  * @param defaultMax 每个IP上能创建的最大连接数
  * @param overrideQuotas 具体指定某IP上最大的连接数，这里指定的最大连接数会覆盖上面maxConnectionsPerIp字段的值
  */
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  // 增加指定的地址记录
  def inc(address: InetAddress) {
    counts.synchronized {
      // 从counts字典获取指定地址当前的连接数
      val count = counts.getOrElseUpdate(address, 0)
      // 更新counts字典
      counts.put(address, count + 1)
      // 根据当时传入的defaultMax及overrideQuotas来获取允许的最大连接数
      val max = overrides.getOrElse(address, defaultMax)
      // 如果过载，就抛出异常
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  // 减少指定的地址记录
  def dec(address: InetAddress) {
    counts.synchronized {
      // 从counts字典获取指定地址当前的连接数
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      // 如果指定地址的连接数已经为1，则可以将其从counts中移除了
      if (count == 1)
        counts.remove(address)
      else
      // 否则更新其计数器减1
        counts.put(address, count - 1)
    }
  }

  // 获取指定地址上的连接数
  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
