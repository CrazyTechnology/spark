

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * An RDD partition used to recover checkpointed data.
  * 用来恢复checkpoint数据的RDD
 */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
 * An RDD that recovers checkpointed data from storage.
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext)
  extends RDD[T](sc, Nil) {

  // CheckpointRDD should not be checkpointed again
  override def doCheckpoint(): Unit = { }
  override def checkpoint(): Unit = { }
  override def localCheckpoint(): this.type = this

  // Note: There is a bug in MiMa that complains about `AbstractMethodProblem`s in the
  // base [[org.apache.spark.rdd.RDD]] class if we do not override the following methods.
  // scalastyle:off
  protected override def getPartitions: Array[Partition] = ???
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
  // scalastyle:on

}
