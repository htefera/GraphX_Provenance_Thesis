package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.LineageContext
import org.apache.spark.rdd.ParallelCollectionRDD

import scala.collection.Map
import scala.reflect.{ClassTag, classTag}

/**
 * Customized version of ParallelCollectionRDD extended to store ParallelCollectionRDD lineage info
 * while creating VertexRDD and EdgeRDD using parallelize method of LineageContext
 *
 */

private[spark] class ParallelCollectionLRDD[T: ClassTag](
    lc: LineageContext,
    @transient val data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
  extends ParallelCollectionRDD[T](lc.sparkContext, data, numSlices, locationPrefs)
  with Lineage[T] {

  override def lineageContext: LineageContext = lc

  override def ttag: ClassTag[T] = classTag[T]
}
