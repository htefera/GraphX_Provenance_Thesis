package org.apache.spark.graphx.lineage
import org.apache.spark.{Dependency, Partition, SparkContext}
import org.apache.spark.graphx.{EdgeRDD, LineageContext, VertexId, VertexRDD}
import org.apache.spark.graphx.impl.{LineageVertexRDDImpl, ShippableVertexPartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Lineage trait for LineageVertexRDD to be used while creating VertexRDD during VertexRDD transformation and actions
 */

trait LineageVertex[VD] extends VertexRDD[VD] {

  implicit protected def vdTag: ClassTag[VD]
  private[graphx] def partitionsRDD: RDD[ShippableVertexPartition[VD]]
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  @transient def lineageContext: LineageContext

  private[graphx] def withPartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]): LineageVertexRDDImpl[VD2]

  private[graphx] def mapVertexPartitions[VD2: ClassTag](
      f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])
  : LineageVertexRDDImpl[VD2]

  def minus(other: RDD[(VertexId, VD)]): LineageVertexRDDImpl[VD]


  def innerZipJoin[U: ClassTag, VD2: ClassTag](other: LineageVertexRDDImpl[U])
      (f: (VertexId, VD, U) => VD2): LineageVertexRDDImpl[VD2]
}

object LineageVertex {

}
