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

package org.apache.spark.executor

import org.apache.spark.util.SparkExitCode._

/**
 * These are exit codes that executors should use to provide the master with information about
 * executor failures assuming that cluster management framework can capture the exit codes (but
 * perhaps not log files). The exit code constants here are chosen to be unlikely to conflict
 * with "natural" exit statuses that may be caused by the JVM or user code. In particular,
 * exit codes 128+ arise on some Unix-likes as a result of signals, and it appears that the
 * OpenJDK JVM may use exit code 1 in some of its own "last chance" code.
 * 假设集群管理框架可以捕获退出代码（但可能不是日志文件），则执行者应使用这些退出代码向主机提供有关执行者故障的信息。
 * 选择此处的退出代码常量，使其不可能与JVM或用户代码可能导致的“自然”退出状态发生冲突。
 * 特别地，由于某些信号，退出代码128+会在某些类Unix上出现，并且看来OpenJDK JVM可能会在其自己的“最后机会”代码中使用退出代码1。
 */
private[spark]
object ExecutorExitCode {

  /** DiskStore failed to create a local temporary directory after many attempts.
   * 多次尝试后，DiskStore无法创建本地临时目录。
   * */
  val DISK_STORE_FAILED_TO_CREATE_DIR = 53

  /** ExternalBlockStore failed to initialize after many attempts.
   * 多次尝试后，ExternalBlockStore未能初始化。 */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE = 54

  /** ExternalBlockStore failed to create a local temporary directory after many attempts. */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR = 55

  /**
   * Executor is unable to send heartbeats to the driver more than
   * "spark.executor.heartbeat.maxFailures" times.
   */
  val HEARTBEAT_FAILURE = 56

  def explainExitCode(exitCode: Int): String = {
    exitCode match {
      case UNCAUGHT_EXCEPTION => "Uncaught exception"
      case UNCAUGHT_EXCEPTION_TWICE => "Uncaught exception, and logging the exception failed"
      case OOM => "OutOfMemoryError"
      case DISK_STORE_FAILED_TO_CREATE_DIR =>
        "Failed to create local directory (bad spark.local.dir?)"
      // TODO: replace external block store with concrete implementation name
      case EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE => "ExternalBlockStore failed to initialize."
      // TODO: replace external block store with concrete implementation name
      case EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR =>
        "ExternalBlockStore failed to create a local temporary directory."
      case HEARTBEAT_FAILURE =>
        "Unable to send heartbeats to driver."
      case _ =>
        "Unknown executor exit code (" + exitCode + ")" + (
          if (exitCode > 128) {
            " (died from signal " + (exitCode - 128) + "?)"
          } else {
            ""
          }
        )
    }
  }
}
