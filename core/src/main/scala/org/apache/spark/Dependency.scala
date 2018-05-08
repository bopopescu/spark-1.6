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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
  * Dependency是抽象类，有一个属性rdd，就是对应RDD的父RDD，所以Dependency就是对父RDD的包装，
  * 并且通过Dependency的类型说明当前这个transformation对应的数据处理方式
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
  *
  * ShuffleDependency的定义相对复杂一些，因为shuffle设计到网络传输，所以要有序列化serializer，
  * 为了减少网络传输，可以加map端聚合，通过mapSideCombine和aggregator控制，
  * 还有key排序相关的keyOrdering，以及重输出的数据如何分区的partitioner，
  * 其他信息包括k,v和combiner的class信息以及shuffleId。shuffle是个相对复杂且开销大的过程，
  * Partition之间的关系在shuffle处戛然而止，因此shuffle是划分stage的依据。
  *
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to None,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 * @param keyOrdering key ordering for RDD's shuffles
  *                    shuffle结果key如何排序
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
  *                   map/reduce端聚合
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
  *                       是否进行map端聚合
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Option[Serializer] = None,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  val shuffleId: Int = _rdd.context.newShuffleId()

  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.size, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  /**
    * OneToOneDependency表示子RDD和父RDD的Partition之间的关系是1对1的，
    * 即子RDD的PartitionId和父RDD的PartitionId一样
    * 如: map,filter算子
    * @param partitionId a partition of the child RDD
    * @return the partitions of the parent RDD that the child partition depends upon
    */
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
  * RangeDependency表示子RDD和父RDD的Partition之间的关系是一个区间内的1对1对应关系
  * union 算子就是RangeDependency
  *
  * 窄依赖的一个实现
  * 子RDD的partition和父RDD的partition在一个范围(range)内,是1对1的关系
  * 例如range为5,子RDD partition开始为3,父RDD partition开始为8,则
  * 子RDD partition index 3 4 5 6 7
  * 父RDD partition index 8 9 10 11 12
  *
  * 求子RDD的partition index 5 的父RDD partition index为
  * 5 - 3 + 8 = 10,对应getParents的代码
  * partitionId - outStart + inStart
  *
  *
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    //检查输入的partitionId是狗在range内
    //根据位置的对应关系,返回父RDD的partition
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
