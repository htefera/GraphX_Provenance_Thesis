package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx.{EdgeRDD, LineageContext, VertexId, VertexRDD}
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.reflect.{ClassTag, classTag}

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
