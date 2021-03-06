
package org.apache.spark.sql.execution.datasources.v2

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.BatchWriteSupport
import org.apache.spark.sql.types.StructType

/**
 * A logical plan representing a data source v2 scan.
 *
 * @param source An instance of a [[DataSourceV2]] implementation.
 * @param options The options for this scan. Used to create fresh [[BatchWriteSupport]].
 * @param userSpecifiedSchema The user-specified schema for this scan.
 */
case class DataSourceV2Relation(
    // TODO: remove `source` when we finish API refactor for write.
    source: TableProvider,
    table: SupportsBatchRead,
    output: Seq[AttributeReference],
    options: Map[String, String],
    userSpecifiedSchema: Option[StructType] = None)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Relation._

  override def name: String = table.name()

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  def newWriteSupport(): BatchWriteSupport = source.createWriteSupport(options, schema)

  def newScanBuilder(): ScanBuilder = {
    val dsOptions = new DataSourceOptions(options.asJava)
    table.newScanBuilder(dsOptions)
  }

  override def computeStats(): Statistics = {
    val scan = newScanBuilder().build()
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        Statistics(sizeInBytes = statistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }
}

/**
 * A specialization of [[DataSourceV2Relation]] with the streaming bit set to true.
 *
 * Note that, this plan has a mutable reader, so Spark won't apply operator push-down for this plan,
 * to avoid making the plan mutable. We should consolidate this plan and [[DataSourceV2Relation]]
 * after we figure out how to apply operator push-down for streaming data sources.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    readSupport: ReadSupport,
    scanConfigBuilder: ScanConfigBuilder)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString(maxFields: Int): String = {
    "Streaming RelationV2 " + metadataString(maxFields)
  }

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && readSupport.getClass == other.readSupport.getClass &&
        options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = readSupport match {
    case r: OldSupportsReportStatistics =>
      val statistics = r.estimateStatistics(scanConfigBuilder.build())
      Statistics(sizeInBytes = statistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  private implicit class SourceHelpers(source: DataSourceV2) {
    def asWriteSupportProvider: BatchWriteSupportProvider = {
      source match {
        case provider: BatchWriteSupportProvider =>
          provider
        case _ =>
          throw new AnalysisException(s"Data source is not writable: $name")
      }
    }

    def name: String = {
      source match {
        case registered: DataSourceRegister =>
          registered.shortName()
        case _ =>
          source.getClass.getSimpleName
      }
    }

    def createWriteSupport(
        options: Map[String, String],
        schema: StructType): BatchWriteSupport = {
      asWriteSupportProvider.createBatchWriteSupport(
        UUID.randomUUID().toString,
        schema,
        SaveMode.Append,
        new DataSourceOptions(options.asJava)).get
    }
  }

  def create(
      provider: TableProvider,
      table: SupportsBatchRead,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(provider, table, output, options, userSpecifiedSchema)
  }

  // TODO: remove this when we finish API refactor for write.
  def createRelationForWrite(
      source: DataSourceV2,
      options: Map[String, String]): DataSourceV2Relation = {
    val provider = source.asInstanceOf[TableProvider]
    val dsOptions = new DataSourceOptions(options.asJava)
    val table = provider.getTable(dsOptions)
    create(provider, table.asInstanceOf[SupportsBatchRead], options)
  }
}
