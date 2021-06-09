package org.apache.spark.graphx.lineage
import org.apache.spark.Partition
import org.apache.spark.graphx.{EdgeRDD, LineageContext, VertexId, VertexRDD}
import org.apache.spark.graphx.impl.ShippableVertexPartition
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

  // TODO customize all methods below

  override def reindex(): VertexRDD[VD] = null

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = null

  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = null

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = null

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = null

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = null

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = null

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = null

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = null

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = null

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = null

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = null

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = null

  override def reverseRoutingTables(): VertexRDD[VD] = null

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = null

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = null

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = null

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = null

  override private[graphx] def shipVertexIds() = null

}

object LineageVertex {

}
