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

package kafka.utils

import java.io.IOException
import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient}
import org.apache.kafka.common.Node

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.kafka.common.utils.{Time => JTime}

object NetworkClientBlockingOps {
  implicit def networkClientBlockingOps(client: NetworkClient): NetworkClientBlockingOps =
    new NetworkClientBlockingOps(client)
}

/**
 * Provides extension methods for `NetworkClient` that are useful for implementing blocking behaviour. Use with care.
 *
 * Example usage:
 *
 * {{{
 * val networkClient: NetworkClient = ...
 * import NetworkClientBlockingOps._
 * networkClient.blockingReady(...)
 * }}}
 */
class NetworkClientBlockingOps(val client: NetworkClient) extends AnyVal {

  /**
   * Invokes `client.ready` followed by 0 or more `client.poll` invocations until the connection to `node` is ready,
   * the timeout expires or the connection fails.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. If the connection fails,
   * an `IOException` is thrown instead.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  def blockingReady(node: Node, timeout: Long)(implicit time: JTime): Boolean = {
    require(timeout >=0, "timeout should be >= 0")
    // ready()方法会在Node节点未准备好时尝试进行连接
    client.ready(node, time.milliseconds()) || pollUntil(timeout) { (_, now) =>
      if (client.isReady(node, now)) // 检测Node是否Ready
        true
      else if (client.connectionFailed(node))
        // 抛出异常
        throw new IOException(s"Connection to $node failed")
      else false
    }
  }

  /**
   * Invokes `client.send` followed by 1 or more `client.poll` invocations until a response is received or a
   * disconnection happens (which can happen for a number of reasons including a request timeout).
   *
   * In case of a disconnection, an `IOException` is thrown.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  def blockingSendAndReceive(request: ClientRequest)(implicit time: JTime): ClientResponse = {
    client.send(request, time.milliseconds()) // 发送请求

    pollContinuously { responses =>
      // 找到上面的发送请求对应的响应，根据请求和响应的correlationId进行匹配
      val response = responses.find { response =>
        response.request.request.header.correlationId == request.request.header.correlationId
      }
      response.foreach { r =>
        if (r.wasDisconnected) {
          // 连接断开，抛出IOException异常
          val destination = request.request.destination
          throw new IOException(s"Connection to $destination was disconnected before the response was read")
        }
      }
      response
    }

  }

  /**
   * Invokes `client.poll` until `predicate` returns `true` or the timeout expires.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. Exceptions thrown via
   * `predicate` are not handled and will bubble up.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  private def pollUntil(timeout: Long)(predicate: (Seq[ClientResponse], Long) => Boolean)(implicit time: JTime): Boolean = {
    val methodStartTime = time.milliseconds()
    // 计算超时时间
    val timeoutExpiryTime = methodStartTime + timeout

    // 递归方法，有超时时间
    @tailrec
    def recursivePoll(iterationStartTime: Long): Boolean = {
      // 剩余超时时间
      val pollTimeout = timeoutExpiryTime - iterationStartTime
      // 发送请求
      val responses = client.poll(pollTimeout, iterationStartTime).asScala
      // 检测是否满足递归结束条件
      if (predicate(responses, iterationStartTime)) true
      else {
        val afterPollTime = time.milliseconds()
        // 未超时，继续进行递归操作
        if (afterPollTime < timeoutExpiryTime) recursivePoll(afterPollTime)
        else false // 超时返回
      }
    }

    // 进入递归方法
    recursivePoll(methodStartTime)
  }

  /**
    * Invokes `client.poll` until `collect` returns `Some`. The value inside `Some` is returned.
    *
    * Exceptions thrown via `collect` are not handled and will bubble up.
    *
    * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
    * care.
    */
  private def pollContinuously[T](collect: Seq[ClientResponse] => Option[T])(implicit time: JTime): T = {

    // 递归方法，没有超时时间
    @tailrec
    def recursivePoll: T = {
      // rely on request timeout to ensure we don't block forever
      // poll操作发送请求，ClientRequest有超时时间，所以此处虽然超时时间是Long.MaxValue，但并不会永远阻塞
      val responses = client.poll(Long.MaxValue, time.milliseconds()).asScala
      collect(responses) match { // 检测是否满足递归结束条件
        // 有结果，返回
        case Some(result) => result
        // 没有结果，进行下一次递归
        case None => recursivePoll
      }
    }
    // 进入递归方法
    recursivePoll
  }

}
