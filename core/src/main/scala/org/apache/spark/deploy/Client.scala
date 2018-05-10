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

package org.apache.spark.deploy

import scala.collection.mutable.HashSet
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import org.apache.log4j.{Level, Logger}

import org.apache.spark.rpc.{RpcEndpointRef, RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.util.{ThreadUtils, SparkExitCode, Utils}

/**
 * Proxy that relays messages to the driver.
 *
 * We currently don't support retry if submission fails. In HA mode, client will submit request to
 * all masters and see which one could handle it.
 */
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoints: Seq[RpcEndpointRef],
    conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  // A scheduled executor used to send messages at the specified time.
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
  // Used to provide the implicit parameter of `Future` methods.
  private val forwardMessageExecutionContext =
    ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // Exit normally
        case e: Throwable =>
          logError(e.getMessage, e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })

   private val lostMasters = new HashSet[RpcAddress]
   private var activeMasterEndpoint: RpcEndpointRef = null

  // ClientEndpoint生命周期中的启动方法
  // 注册Driver程序
  override def onStart(): Unit = {
    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.

        // Spark使用DriverWrapper启动用户APP的main函数，而不是直接启动，
        // 这是为了Driver程序和启动Driver的Worker程序共命运(源码注释中称为share fate)，
        // 即如果此Worker挂了，对应的Driver也会停止
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = "spark.driver.extraClassPath"
        val classPathEntries = sys.props.get(classPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = "spark.driver.extraLibraryPath"
        val libraryPathEntries = sys.props.get(libraryPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = "spark.driver.extraJavaOptions"
        val extraJavaOpts = sys.props.get(extraJavaOptsConf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)
        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)

        // Driver信息的格式
        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command)  //command中包含mainClass,arguments,environment... ,mainClass为DriverWrapper

        // 向Master发送RequestSubmitDriver消息,注册Driver
        // 如果注册成功,Master回复SubmitDriverResponse
        ayncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        val driverId = driverArgs.driverId
        ayncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
  }

  /**
   * Send the message to master and forward the reply to self asynchronously.
   */
  private def ayncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit = {
    // 向masterEndpoints注册,并获得回复
    for (masterEndpoint <- masterEndpoints) {
      masterEndpoint.ask[T](message).onComplete {
        case Success(v) => self.send(v)
        case Failure(e) =>
          logWarning(s"Error sending messages to master $masterEndpoint", e)
      }(forwardMessageExecutionContext)
    }
  }

  /* Find out driver status then exit the JVM */
  def pollAndReportStatus(driverId: String) {
    // Since ClientEndpoint is the only RpcEndpoint in the process, blocking the event loop thread
    // is fine.
    logInfo("... waiting before polling master for driver state")
    Thread.sleep(5000)
    logInfo("... polling master for driver state")
    val statusResponse =
      activeMasterEndpoint.askWithRetry[DriverStatusResponse](RequestDriverStatus(driverId))
    statusResponse.found match {
      case false =>
        logError(s"ERROR: Cluster master did not recognize $driverId")
        System.exit(-1)
      case true =>
        logInfo(s"State of $driverId is ${statusResponse.state.get}")
        // Worker node, if present
        (statusResponse.workerId, statusResponse.workerHostPort, statusResponse.state) match {
          case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
            logInfo(s"Driver running on $hostPort ($id)")
          case _ =>
        }
        // Exception, if present
        statusResponse.exception.map { e =>
          logError(s"Exception from cluster was: $e")
          e.printStackTrace()
          System.exit(-1)
        }
        System.exit(0)
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    // 接受Master发来的Driver注册成功消息
    case SubmitDriverResponse(master, success, driverId, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        pollAndReportStatus(driverId.get)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }


    case KillDriverResponse(master, driverId, success, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        pollAndReportStatus(driverId)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master $remoteAddress.")
      lostMasters += remoteAddress
      // Note that this heuristic does not account for the fact that a Master can recover within
      // the lifetime of this client. Thus, once a Master is lost it is lost to us forever. This
      // is not currently a concern, however, because this client does not retry submissions.
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master ($remoteAddress).")
      logError(s"Cause was: $cause")
      lostMasters += remoteAddress
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onError(cause: Throwable): Unit = {
    logError(s"Error processing messages, exiting.")
    cause.printStackTrace()
    System.exit(-1)
  }

  override def onStop(): Unit = {
    forwardMessageThread.shutdownNow()
  }
}

/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object Client {
  def main(args: Array[String]) {
    // scalastyle:off println
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    // scalastyle:on println

    val conf = new SparkConf()

    //通过ClientArguments类解析Client模式传入的参数
    val driverArgs = new ClientArguments(args)

    if (!driverArgs.logLevel.isGreaterOrEqual(Level.WARN)) {
      conf.set("spark.akka.logLifecycleEvents", "true")
    }
    conf.set("spark.rpc.askTimeout", "10")
    conf.set("akka.loglevel", driverArgs.logLevel.toString.replace("WARN", "WARNING"))
    Logger.getRootLogger.setLevel(driverArgs.logLevel)

    // 创建RpcEnv对象
    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))

    // 获得和Master通信的RpcEndpointRef
    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, _, Master.ENDPOINT_NAME))

    // 注册ClientEndPoint
    // ClientEndPoint生命周期方法onStart使用masterEndPoints,发送RequestSubmitDriver消息给Master,注册Driver
    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints, conf))

    rpcEnv.awaitTermination()
  }
}
