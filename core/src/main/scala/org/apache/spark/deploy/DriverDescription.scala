
package org.apache.spark.deploy

/**
 *Driver 的描述信息
 */
private[deploy] case class DriverDescription(
    jarUrl: String,
    mem: Int,
    cores: Int,
    supervise: Boolean,
    command: Command) {

  override def toString: String = s"DriverDescription (${command.mainClass})"
}
