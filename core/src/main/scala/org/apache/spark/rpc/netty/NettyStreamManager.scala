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

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc.RpcEnvFileServer
import org.apache.spark.util.Utils

/**
 * StreamManager implementation for serving files from a NettyRpcEnv.
  * 作用于NettyRpcEnv,提供file下载服务
 */
private[netty] class NettyStreamManager(rpcEnv: NettyRpcEnv)
  extends StreamManager with RpcEnvFileServer {

  // NettyStreamManager和HttpBasedFileServer不同，其没有将文件写入本地，
  // 而是使用两个属性jars和files集合保存，对应的addJar方法就是将File对象添加到集合jars中，
  // 下载文件依靠openStream方法
  private val files = new ConcurrentHashMap[String, File]()
  private val jars = new ConcurrentHashMap[String, File]()

  override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    throw new UnsupportedOperationException()
  }

  //openStream方法继承自StreamManager，StreamManager提供底层文件的下载服务，如shuffle过程中间结果的下载
  override def openStream(streamId: String): ManagedBuffer = {
    val Array(ftype, fname) = streamId.stripPrefix("/").split("/", 2)
    val file = ftype match {
      case "files" => files.get(fname)
      case "jars" => jars.get(fname)
      case _ => throw new IllegalArgumentException(s"Invalid file type: $ftype")
    }

    require(file != null && file.isFile(), s"File not found: $streamId")
    new FileSegmentManagedBuffer(rpcEnv.transportConf, file, 0, file.length())
  }

  override def addFile(file: File): String = {
    require(files.putIfAbsent(file.getName(), file) == null,
      s"File ${file.getName()} already registered.")
    s"${rpcEnv.address.toSparkURL}/files/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

  override def addJar(file: File): String = {
    require(jars.putIfAbsent(file.getName(), file) == null,
      s"JAR ${file.getName()} already registered.")
    s"${rpcEnv.address.toSparkURL}/jars/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

}
