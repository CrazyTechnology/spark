package org.apache.spark.executor

import org.apache.spark.{TaskCommitDenied, TaskFailedReason}

/**
 * Exception thrown when a task attempts to commit output to HDFS but is denied by the driver.
 */
private[spark] class CommitDeniedException(
    msg: String,
    jobID: Int,
    splitID: Int,
    attemptNumber: Int)
  extends Exception(msg) {

  def toTaskFailedReason: TaskFailedReason = TaskCommitDenied(jobID, splitID, attemptNumber)
}
