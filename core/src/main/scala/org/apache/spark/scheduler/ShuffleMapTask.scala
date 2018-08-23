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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // 反序列化RDD

    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
//    解压有driver端发送过来的RDD 及 这个RDD之后的那个RDD的依赖信息，关于这点，你可以回过头来看一下提交stage的时候的代码
// 从广播变量中反序列化出finalRDD和dependency
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
//    2. shuffleWriter用于写计算后的数据
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // shuffle manager是为 shuffle Spark 系统抽象出来的可插拔的接口被创建在`
      // Driver 和 Executor 上 具体是在SparkEnv实例化的时候进行配置的
//      从manager中获取Writer，这里获取的是HashShuffleWriter
//      3. 采用哪一种shuffle机制，目前有Hash，Sorted，Tungsten三种，默认采用的Sorted的方式。这个是在创建SparkEnv的时候指定的
//      4. 获取相应的writer
//      5. 将计算后的数据写入内存或者磁盘，当然也有可能spill到磁盘。
//      6.关闭writer，并返回这个map是否完成的status信息
      // 获取shuffleManager
//      其中的finalRDD和dependency是在Driver端DAGScheluer中提交Stage的时候加入广播变量的。
//      接着通过SparkEnv获取shuffleManager，默认使用的是sort（对应的是org.apache.spark.shuffle.sort.SortShuffleManager），可通过spark.shuffle.manager设置
      val manager = SparkEnv.get.shuffleManager
      // 通过shuffleManager的getWriter()方法，获得shuffle的writer
//      接着通过SparkEnv获取shuffleManager，默认使用的是sort（对应的是org.apache.spark.shuffle.sort.SortShuffleManager），可通过spark.shuffle.manager设置。
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 通过rdd指定分区的迭代器iterator方法来遍历每一条数据，再之上再调用writer的write方法以写数据
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
