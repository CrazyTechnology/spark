
package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf

/**
 * An interface for all the broadcast implementations in Spark (to allow
 * multiple broadcast implementations). SparkContext uses a user-specified
 * BroadcastFactory implementation to instantiate a particular broadcast for the
 * entire Spark job.
 * Spark中所有广播实现的接口（以允许多个广播实现）。
 * SparkContext使用用户指定的BroadcastFactory实现实例化整个Spark作业的特定广播。
 */
private[spark] trait BroadcastFactory {

  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager): Unit

  /**
   * Creates a new broadcast variable.
   *
   * @param value value to broadcast  广播变量的值
   * @param isLocal whether we are in local mode (single JVM process) 是否是local模式
   * @param id unique id representing this broadcast variable  广播变量的id
   */
  def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  def stop(): Unit
}
