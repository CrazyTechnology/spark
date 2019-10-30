
package org.apache.spark.sql.sources

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * Data sources should implement this trait so that they can register an alias to their data source.
 * This allows users to give the data source alias as the format type over the fully qualified
 * class name.
 * 数据源应实现此特征，以便它们可以向其数据源注册别名。 这样，用户可以在完全限定的类名上为数据源别名提供格式类型的别名。
 * A new instance of this class will be instantiated each time a DDL call is made.
 * 每次进行DDL调用时都会实例化此类的新实例。
 *
 * @since 1.5.0
 */
@InterfaceStability.Stable
trait DataSourceRegister {

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   * 表示此数据源提供程序使用的格式的字符串。 子级将其覆盖以为数据源提供一个很好的别名。 例如：
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  def shortName(): String
}

/**
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
 * 由为特定类型的数据源产生关系的对象实现。 当为Spark SQL提供指定了USING子句的DDL操作（以指定实现的RelationProvider）时，
 * 此接口用于传递用户指定的参数。
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 * 用户可以指定给定数据源的标准类名。 当找不到该类时，Spark SQL会将类名称DefaultDefault附加到路径中，以减少冗长的调用。
 * 例如，“ org.apache.spark.sql.json”将解析为数据源“ org.apache.spark.sql.json.DefaultSource”
 * A new instance of this class will be instantiated each time a DDL call is made.
 * 每次进行DDL调用时都会实例化此类的新实例。
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

/**
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema.  When Spark SQL is given a DDL operation with a USING clause specified (
 * to specify the implemented SchemaRelationProvider) and a user defined schema, this interface
 * is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[SchemaRelationProvider]] is that
 * users need to provide a schema when using a [[SchemaRelationProvider]].
 * A relation provider can inherits both [[RelationProvider]] and [[SchemaRelationProvider]]
 * if it can support both schema inference and user-specified schemas.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait SchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined schema.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation
}

/**
 * ::Experimental::
 * Implemented by objects that can produce a streaming `Source` for a specific format or system.
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Unstable
trait StreamSourceProvider {

  /**
   * Returns the name and schema of the source that can be used to continually read data.
   * @since 2.0.0
   */
  def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType)

  /**
   * @since 2.0.0
   */
  def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source
}

/**
 * ::Experimental::
 * Implemented by objects that can produce a streaming `Sink` for a specific format or system.
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Unstable
trait StreamSinkProvider {
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait CreatableRelationProvider {
  /**
   * Save the DataFrame to the destination and return a relation with the given parameters based on
   * the contents of the given DataFrame. The mode specifies the expected behavior of createRelation
   * when data already exists.
   * Right now, there are three modes, Append, Overwrite, and ErrorIfExists.
   * Append mode means that when saving a DataFrame to a data source, if data already exists,
   * contents of the DataFrame are expected to be appended to existing data.
   * Overwrite mode means that when saving a DataFrame to a data source, if data already exists,
   * existing data is expected to be overwritten by the contents of the DataFrame.
   * ErrorIfExists mode means that when saving a DataFrame to a data source,
   * if data already exists, an exception is expected to be thrown.
   *
   * @since 1.3.0
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation
}

/**
 * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
 * be able to produce the schema of their data in the form of a `StructType`. Concrete
 * implementation should inherit from one of the descendant `Scan` classes, which define various
 * abstract methods for execution.
 * 表示具有已知架构的元组的集合。
 * 扩展BaseRelation的类必须能够以`StructType`的形式产生其数据模式。
 * 具体的实现应继承自后代Scan类之一，该类定义了各种抽象的执行方法。
 * BaseRelations must also define an equality function that only returns true when the two
 * instances will return the same data. This equality function is used when determining when
 * it is safe to substitute cached results for a given relation.
 * BaseRelations还必须定义一个相等函数，该函数仅在两个实例将返回相同数据时才返回true。
 * 在确定何时可以安全地用缓存的结果替换给定关系时使用此相等函数。
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType

  /**
   * Returns an estimated size of this relation in bytes. This information is used by the planner
   * to decide when it is safe to broadcast a relation and can be overridden by sources that
   * know the size ahead of time. By default, the system will assume that tables are too
   * large to broadcast. This method will be called multiple times during query planning
   * and thus should not perform expensive operations for each invocation.
   * 返回此关系的估计大小（以字节为单位）。 计划人员可以使用此信息来确定何时安全广播关系，并且可以由事先知道其大小的消息源覆盖。
   * 默认情况下，系统将假定表太大而无法广播。 在查询计划期间将多次调用此方法，因此不应为每次调用执行昂贵的操作。
   *
   * @note It is always better to overestimate size than underestimate, because underestimation
   * could lead to execution plans that are suboptimal (i.e. broadcasting a very large table).
   *       总是高估大小总比低估好，因为低估可能会导致执行计划不理想（即广播很大的表）。
   * @since 1.3.0
   */
  def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes

  /**
   * Whether does it need to convert the objects in Row to internal representation, for example:
   * 是否需要将Row中的对象转换为内部表示，例如：
   *  java.lang.String to UTF8String
   *  java.lang.Decimal to Decimal
   *
   * If `needConversion` is `false`, buildScan() should return an `RDD` of `InternalRow`
   * 如果“ needConversion”为“ false”，则buildScan（）应该返回“ InternalRow”的“ RDD”。
   *
   * @note The internal representation is not stable across releases and thus data sources outside
   * of Spark SQL should leave this as true.
   *       内部表示形式在各个发行版中不稳定，因此Spark SQL外部的数据源应将其保留为true。
   * @since 1.4.0
   */
  def needConversion: Boolean = true

  /**
   * Returns the list of [[Filter]]s that this datasource may not be able to handle.
   * These returned [[Filter]]s will be evaluated by Spark SQL after data is output by a scan.
   * By default, this function will return all filters, as it is always safe to
   * double evaluate a [[Filter]]. However, specific implementations can override this function to
   * avoid double filtering when they are capable of processing a filter internally.
   *
   * @since 1.6.0
   */
  def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters
}

/**
 * A BaseRelation that can produce all of its tuples as an RDD of Row objects.
 * 可以将其所有元组生成为Row对象的RDD的BaseRelation。
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait TableScan {
  def buildScan(): RDD[Row]
}

/**
 * A BaseRelation that can eliminate unneeded columns before producing an RDD
 * containing all of its tuples as Row objects.
 * 一个BaseRelation，可以在生成包含其所有元组作为Row对象的RDD之前消除不需要的列。
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait PrunedScan {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

/**
 * A BaseRelation that can eliminate unneeded columns and filter using selected
 * predicates before producing an RDD containing all matching tuples as Row objects.
 * 一个BaseRelation，它可以消除不需要的列，并在生成包含所有匹配元组作为Row对象的RDD之前使用选定的谓词进行过滤。
 * The actual filter should be the conjunction of all `filters`,
 * i.e. they should be "and" together.
 *
 * The pushed down filters are currently purely an optimization as they will all be evaluated
 * again.  This means it is safe to use them with methods that produce false positives such
 * as filtering partitions based on a bloom filter.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait PrunedFilteredScan {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

/**
 * A BaseRelation that can be used to insert data into it through the insert method.
 * If overwrite in insert method is true, the old data in the relation should be overwritten with
 * the new data. If overwrite in insert method is false, the new data should be appended.
 *
 * InsertableRelation has the following three assumptions.
 * 1. It assumes that the data (Rows in the DataFrame) provided to the insert method
 * exactly matches the ordinal of fields in the schema of the BaseRelation.
 * 2. It assumes that the schema of this relation will not be changed.
 * Even if the insert method updates the schema (e.g. a relation of JSON or Parquet data may have a
 * schema update after an insert operation), the new schema will not be used.
 * 3. It assumes that fields of the data provided in the insert method are nullable.
 * If a data source needs to check the actual nullability of a field, it needs to do it in the
 * insert method.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}

/**
 * ::Experimental::
 * An interface for experimenting with a more direct connection to the query planner.  Compared to
 * [[PrunedFilteredScan]], this operator receives the raw expressions from the
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].  Unlike the other APIs this
 * interface is NOT designed to be binary compatible across releases and thus should only be used
 * for experimentation.
 *
 * @since 1.3.0
 */
@Experimental
@InterfaceStability.Unstable
trait CatalystScan {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}
