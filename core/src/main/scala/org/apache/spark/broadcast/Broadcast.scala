
package org.apache.spark.broadcast

import java.io.Serializable

import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
 * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
 * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
 * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
 * communication cost.
 *
 * Broadcast variables are created from a variable `v` by calling
 * [[org.apache.spark.SparkContext#broadcast]].
 * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
 * `value` method. The interpreter session below shows this:
 *
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 * After the broadcast variable is created, it should be used instead of the value `v` in any
 * functions run on the cluster so that `v` is not shipped to the nodes more than once.
 * In addition, the object `v` should not be modified after it is broadcast in order to ensure
 * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
 * to a new node later).
 *
 * @param id A unique identifier for the broadcast variable. 广播变量的唯一标识
 * @tparam T Type of the data contained in the broadcast variable.  广播变量中的数据类型
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * Flag signifying whether the broadcast variable is valid
   * (that is, not already destroyed) or not.
    * 判断broadcast是否有效
   */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** Get the broadcasted value.
    * 获取广播变量的值 */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
    * 异步删除executor上的缓存副本
   * Asynchronously delete cached copies of this broadcast on the executors.
    * 如果删除后想要重新使用，必须重新发送
   * If the broadcast is used after this is called, it will need to be re-sent to each executor.
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   * Delete cached copies of this broadcast on the executors. If the broadcast is used after
   * this is called, it will need to be re-sent to each executor.
   * @param blocking Whether to block until unpersisting has completed
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
    * Destroy all data and metadata related to this broadcast variable. Use this with caution;
    * 销毁与此广播变量相关的所有数据和元数据。请谨慎使用;
    * once a broadcast variable has been destroyed, it cannot be used again.
    * 一旦broadcast变量被销毁，就不能再次被使用了
    * This method blocks until destroy has completed
    * 此方法将阻塞，直到销毁完成
    */
  def destroy(): Unit = {
    destroy(blocking = true)
  }

  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * @param blocking Whether to block until destroy has completed
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    //销毁时是否锁住
    doDestroy(blocking)
  }

  /**
    * 判断广播变量是否还能使用
   * Whether this Broadcast is actually usable. This should be false once persisted state is
   * removed from the driver.
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * Actually get the broadcasted value. Concrete implementations of Broadcast class must
   * define their own way to get the value.
    * 获取broadcast的值
    * Broadcast是个抽象类，里面的方法需要子类来实现
   */
  protected def getValue(): T

  /**
   * Actually unpersist the broadcasted value on the executors. Concrete implementations of
   * Broadcast class must define their own logic to unpersist their own data.
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * Actually destroy all data and metadata related to this broadcast variable.
   * Implementation of Broadcast class must define their own logic to destroy their own
   * state.
    * 由它的实现类来具体实现销毁方法
   */
  protected def doDestroy(blocking: Boolean)

  /** Check if this broadcast is valid. If not valid, exception is thrown. */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  override def toString: String = "Broadcast(" + id + ")"
}
