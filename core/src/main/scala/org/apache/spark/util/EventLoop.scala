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

package org.apache.spark.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import scala.util.control.NonFatal

import org.apache.spark.Logging

/**
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
 *
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
  *
  * DAGScheduler使用EventLoop(LinkedBlockingDeque)异步处理Job的流程
  *
  * 异步处理借助于EventLoop实现，EventLoop内部维护了LinkedBlockingDeque，
  * LinkedBlockingDeque是基于链表实现的双端阻塞队列，参考LinkedBlockingDeque.java，
  * LinkedBlockingDeque支持双端同时操作，在指定容量并且容量已满时，支持阻塞。
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  // 双端阻塞队列
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  private val stopped = new AtomicBoolean(false)

  // eventThread中循环消费eventQueue中存储的事件
  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {
            onReceive(event)
          } catch {
            case NonFatal(e) => {
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
            }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
   * Put the event into the event queue. The event thread will process it later.
   */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  /**
   * Return if the event thread has already been started but not yet stopped.
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * Invoked when `start()` is called but before the event thread starts.
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
   */
  protected def onStop(): Unit = {}

  /**
   * Invoked in the event thread when polling events from the event queue.
   *
   * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
   * and cannot process events in time. If you want to call some blocking actions, run them in
   * another thread.
    *
    * 消费方法为onReceive，是抽象方法，具体逻辑由子类实现。
   */
  protected def onReceive(event: E): Unit

  /**
   * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
   * will be ignored.
   */
  protected def onError(e: Throwable): Unit

}
