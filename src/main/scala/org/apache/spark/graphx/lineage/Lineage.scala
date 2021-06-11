package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.LineageContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Lineage trait for LineageRDD to be used while creating VertexRDD and EdgeRDD from parallelize method of Lineagecontext
 * Similar to Titian lineage to manage RDD operations
 */

trait Lineage[T] extends RDD[T]  {

  implicit def ttag: ClassTag[T]

  @transient def lineageContext: LineageContext
}

object Lineage {

}