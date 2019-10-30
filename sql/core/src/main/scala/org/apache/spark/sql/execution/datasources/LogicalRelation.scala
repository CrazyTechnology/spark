package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 * 用于将[[BaseRelation]]链接到逻辑查询计划。
 * Note that sometimes we need to use `LogicalRelation` to replace an existing leaf node without
 * changing the output attributes' IDs.  The `expectedOutputAttributes` parameter is used for
 * this purpose.  See https://issues.apache.org/jira/browse/SPARK-10741 for more details.
 * 注意，有时我们需要使用“ LogicalRelation”来替换现有的叶子节点，而无需更改输出属性的ID。
 * “ expectedOutputAttributes”参数用于此目的。
 */
case class LogicalRelation(
    relation: BaseRelation,
    expectedOutputAttributes: Option[Seq[Attribute]] = None,
    catalogTable: Option[CatalogTable] = None)
  extends LeafNode with MultiInstanceRelation {

  override val output: Seq[AttributeReference] = {
    val attrs = relation.schema.toAttributes
    expectedOutputAttributes.map { expectedAttrs =>
      assert(expectedAttrs.length == attrs.length)
      attrs.zip(expectedAttrs).map {
        // We should respect the attribute names provided by base relation and only use the
        // exprId in `expectedOutputAttributes`.
        // The reason is that, some relations(like parquet) will reconcile attribute names to
        // workaround case insensitivity issue.
        case (attr, expected) => attr.withExprId(expected.exprId)
      }
    }.getOrElse(attrs)
  }

  // Logical Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any): Boolean = other match {
    case l @ LogicalRelation(otherRelation, _, _) => relation == otherRelation && output == l.output
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(relation, output)
  }

  override def sameResult(otherPlan: LogicalPlan): Boolean = {
    otherPlan.canonicalized match {
      case LogicalRelation(otherRelation, _, _) => relation == otherRelation
      case _ => false
    }
  }

  // When comparing two LogicalRelations from within LogicalPlan.sameResult, we only need
  // LogicalRelation.cleanArgs to return Seq(relation), since expectedOutputAttribute's
  // expId can be different but the relation is still the same.
  override lazy val cleanArgs: Seq[Any] = Seq(relation)

  @transient override lazy val statistics: Statistics = {
    catalogTable.flatMap(_.stats.map(_.copy(sizeInBytes = relation.sizeInBytes))).getOrElse(
      Statistics(sizeInBytes = relation.sizeInBytes))
  }

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): this.type = {
    LogicalRelation(
      relation,
      expectedOutputAttributes.map(_.map(_.newInstance())),
      catalogTable).asInstanceOf[this.type]
  }

  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.location.refresh()
    case _ =>  // Do nothing.
  }

  override def simpleString: String = s"Relation[${Utils.truncatedString(output, ",")}] $relation"
}
