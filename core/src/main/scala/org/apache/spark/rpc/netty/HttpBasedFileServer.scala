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

import org.apache.spark.{HttpFileServer, SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEnvFileServer

private[netty] class HttpBasedFileServer(conf: SparkConf, securityManager: SecurityManager)
  extends RpcEnvFileServer {

  @volatile private var httpFileServer: HttpFileServer = _

  /**
    * https://blog.csdn.net/u011564172/article/details/64214915
    * 1.driver程序启动，初始化SparkContext时启动jetty server，流程如上图①至⑨，
    * 其中jetty server文件服务器的根目录为HttpFileServer的baseDir目录(默认为/tmp/UUID)，
    * 下面有两个二级目录jars、files分别存放jar和file。
    *
    * 2.调用SparkContext的addJar方法依次将driver程序和spark-submit中指定的jar包copy到jetty
    * 文件服务器的baseDir/jars目录下，如上图②⑩⑪⑫流程。
    *
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    *
    *  添加file到文件服务器,用于exector下载
    */
  override def addFile(file: File): String = {
    getFileServer().addFile(file)
  }

  /**
    * 同上 addFile
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    *  添加jar到文件拂去其,用于Executor下载
    */
  override def addJar(file: File): String = {
    getFileServer().addJar(file)
  }

  def shutdown(): Unit = {
    if (httpFileServer != null) {
      httpFileServer.stop()
    }
  }

  private def getFileServer(): HttpFileServer = {
    if (httpFileServer == null) synchronized {
      if (httpFileServer == null) {
        httpFileServer = startFileServer()
      }
    }
    httpFileServer
  }

  private def startFileServer(): HttpFileServer = {
    val fileServerPort = conf.getInt("spark.fileserver.port", 0)
    val server = new HttpFileServer(conf, securityManager, fileServerPort)
    server.initialize()
    server
  }

}
