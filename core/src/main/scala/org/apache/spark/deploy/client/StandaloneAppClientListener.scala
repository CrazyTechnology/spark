
package org.apache.spark.deploy.client

/**
 * Callbacks invoked by deploy client when various events happen. There are currently four events:
 * connecting to the cluster, disconnecting, being given an executor, and having an executor
 * removed (either due to failure or due to revocation).
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait StandaloneAppClientListener {
  def connected(appId: String): Unit

  /** Disconnection may be a temporary state, as we fail over to a new Master. */
  def disconnected(): Unit

  /** An application death is an unrecoverable failure condition. */
  def dead(reason: String): Unit

  def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit
}
