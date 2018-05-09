/*
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

package org.apache.spark.rpc.netty

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 * An [[RpcEndpoint]] for remote [[RpcEnv]]s to query if an [[RpcEndpoint]] exists.
 *
 * This is used when setting up a remote endpoint reference.
  *
  * RpcEndpointVerifier的作用是，当RpcEndpointRef访问对应的RpcEndpoint前，判断RpcEndpoint是否存在。
  * RpcEndpoint 的实现类,就意味着和Master、Worker等RpcEndpoint实现类处理消息的流程一致
 */
private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  // 判断Dispatcher中是否有对应那么的RpcEndpoint,返回Boolean
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

private[netty] object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an [[RpcEndpoint]] exists. */
  // 消息类型
  case class CheckExistence(name: String)
}
