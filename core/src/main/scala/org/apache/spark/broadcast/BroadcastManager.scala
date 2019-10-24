
package org.apache.spark.broadcast

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.{SecurityManager, SparkConf}

import scala.reflect.ClassTag

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  //使用之前 sparkcontext 和executor会回调
  private def initialize() {
    synchronized {
      if (!initialized) {
        //实例化TorrentBroadcastFactory 工厂
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
