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

package org.apache.spark.sql.execution.datasources

import java.io.{FileNotFoundException, IOException}

import scala.collection.mutable

import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.{Partition => RDDPartition, TaskContext, TaskKilledException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

/**
 * A part (i.e. "block") of a single file that should be read, along with partition column values
 * that need to be prepended to each row.
 * 应该读取的单个文件的一部分（即“块”）以及需要在每行之前添加的分区列值.
 * @param partitionValues value of partition columns to be prepended to each row. 每一行前面的分区列的值。
 * @param filePath path of the file to read 读取文件的路径
 * @param start the beginning offset (in bytes) of the block. 块的起始偏移量（以字节为单位）
 * @param length number of bytes to read. 读取的字节数。
 * @param locations locality information (list of nodes that have the data). 位置信息(拥有数据的节点信息)
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long,
    @transient locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * A collection of file blocks that should be read as a single task
 * 应作为单个任务读取的文件块的集合
 * (possibly from multiple partitioned directories).
 * (可能来自多个分区目录)
 */
case class FilePartition(index: Int, files: Seq[PartitionedFile]) extends RDDPartition

/**
 * An RDD that scans a list of file partitions.
 *扫描文件分区列表的RDD实现类
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  //是否忽略损坏的文件
  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  //是否忽略丢失的文件
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback =
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }
      def next(): Object = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        // don't need to run this `if` for every record.
        val preNumRecordsRead = inputMetrics.recordsRead
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          incTaskInputMetricsBytesRead()
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          // too costly to update every record
          if (inputMetrics.recordsRead %
              SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
            incTaskInputMetricsBytesRead()
          }
          inputMetrics.incRecordsRead(1)
        }
        nextElement
      }

      private def readCurrentFile(): Iterator[InternalRow] = {
        try {
          readFunction(currentFile)
        } catch {
          case e: FileNotFoundException =>
            throw new FileNotFoundException(
              e.getMessage + "\n" +
                "It is possible the underlying files have been updated. " +
                "You can explicitly invalidate the cache in Spark by " +
                "running 'REFRESH TABLE tableName' command in SQL or " +
                "by recreating the Dataset/DataFrame involved.")
        }
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          logInfo(s"Reading File $currentFile")
          // Sets InputFileBlockHolder for the file block's information
          InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

          if (ignoreMissingFiles || ignoreCorruptFiles) {
            currentIterator = new NextIterator[Object] {
              // The readFunction may read some bytes before consuming the iterator, e.g.,
              // vectorized Parquet reader. Here we use lazy val to delay the creation of
              // iterator so that we will throw exception in `getNext`.
              private lazy val internalIter = readCurrentFile()

              override def getNext(): AnyRef = {
                try {
                  if (internalIter.hasNext) {
                    internalIter.next()
                  } else {
                    finished = true
                    null
                  }
                } catch {
                  case e: FileNotFoundException if ignoreMissingFiles =>
                    logWarning(s"Skipped missing file: $currentFile", e)
                    finished = true
                    null
                  // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
                  case e: FileNotFoundException if !ignoreMissingFiles => throw e
                  case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                    logWarning(
                      s"Skipped the rest of the content in the corrupted file: $currentFile", e)
                    finished = true
                    null
                }
              }

              override def close(): Unit = {}
            }
          } else {
            currentIterator = readCurrentFile()
          }

          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              val message = "Parquet column cannot be converted in " +
                s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
                s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
              throw new QueryExecutionException(message, e)
            case e: ParquetDecodingException =>
              if (e.getMessage.contains("Can not read value at")) {
                val message = "Encounter error while reading parquet files. " +
                  "One possible cause: Parquet column cannot be converted in the " +
                  "corresponding files. Details: "
                throw new QueryExecutionException(message, e)
              }
              throw e
          }
        } else {
          currentFile = null
          InputFileBlockHolder.unset()
          false
        }
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  //获取数据最优位置
  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {

    //返回 Seq[PartitionedFile]
    val files = split.asInstanceOf[FilePartition].files

    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        //(host,length)
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    //获取前3个主机，其中要检索的数据最多
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }
  }
}
