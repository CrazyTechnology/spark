

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Marker trait to identify the shape in which tuples are broadcasted. Typical examples of this are
 * identity (tuples remain unchanged) or hashed (tuples are converted into some hash index).
 */
trait BroadcastMode {
  def transform(rows: Array[InternalRow]): Any

  def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any

  def canonicalized: BroadcastMode
}

/**
 * IdentityBroadcastMode requires that rows are broadcasted in their original form.
 */
case object IdentityBroadcastMode extends BroadcastMode {
  // TODO: pack the UnsafeRows into single bytes array.
  override def transform(rows: Array[InternalRow]): Array[InternalRow] = rows

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): Array[InternalRow] = rows.toArray

  override def canonicalized: BroadcastMode = this
}
