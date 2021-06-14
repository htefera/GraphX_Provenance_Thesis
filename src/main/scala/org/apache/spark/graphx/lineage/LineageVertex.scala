package org.apache.spark.graphx.lineage
import org.apache.spark.{Dependency, Partition, SparkContext}
import org.apache.spark.graphx.impl.{ShippableVertexPartition, VertexAttributeBlock}
import org.apache.spark.graphx.{LineageContext, PartitionID, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

/**
 * Lineage trait for LineageVertexRDD to be used while creating VertexRDD during VertexRDD transformation
 */

trait LineageVertex[VD] extends VertexRDD[VD] {

  @transient def lineageContext: LineageContext
  override implicit protected def vdTag: ClassTag[VD]
  override private[graphx] def partitionsRDD: RDD[ShippableVertexPartition[VD]]
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override val partitioner = partitionsRDD.partitioner

  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  override protected def getPreferredLocations(s: Partition): Seq[String] = partitionsRDD.preferredLocations(s)


  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def checkpoint(): Unit = {
    partitionsRDD.checkpoint()
  }

  /** The number of vertices in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_.size.toLong).reduce(_ + _)
  }

  override def isCheckpointed: Boolean = {
    firstParent[ShippableVertexPartition[VD]].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    partitionsRDD.getCheckpointFile
  }

  override def reverseRoutingTables(): LineageVertexRDD[VD] =
    this.mapVertexPartitions(vPart => vPart.withRoutingTable(vPart.routingTable.reverse))


  // Methods to be implemented by the RDDs

  override def aggregateUsingIndex[VD2: ClassTag](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): LineageVertexRDD[VD2]

  def reindex(): LineageVertexRDD[VD]

  def cache(): this.type

  def diff(other: RDD[(VertexId, VD)]): LineageVertexRDD[VD]

  def diff(other: LineageVertexRDD[VD]): LineageVertexRDD[VD]

  def diff(other: VertexRDD[VD]): LineageVertexRDD[VD] = null // TODO remove it

  def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)]) (f: (VertexId, VD, U) => VD2): LineageVertexRDD[VD2]

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
                                                       (f: (VertexId, VD, U) => VD2): LineageVertexRDD[VD2] = null // TODO remove it

  def innerZipJoin[U: ClassTag, VD2: ClassTag](other: LineageVertexRDD[U])(f: (VertexId, VD, U) => VD2): LineageVertexRDD[VD2]

  def leftJoin[VD2: ClassTag, VD3: ClassTag] (other: RDD[(VertexId, VD2)]) (f: (VertexId, VD, Option[VD2]) => VD3) : LineageVertexRDD[VD3]


  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag] (other: VertexRDD[VD2])
                                                         (f: (VertexId, VD, Option[VD2]) => VD3): LineageVertexRDD[VD3] = null // Todo remove it

  def leftZipJoin[VD2: ClassTag, VD3: ClassTag] (other: LineageVertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): LineageVertexRDD[VD3]

  def mapValues[VD2: ClassTag](f: VD => VD2): LineageVertexRDD[VD2]

  def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): LineageVertexRDD[VD2]

  def mapVertexPartitions[VD2: ClassTag] (f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]) : LineageVertexRDD[VD2]

  def minus(other: RDD[(VertexId, VD)]): LineageVertexRDD[VD]

  def minus (other: LineageVertexRDD[VD]): LineageVertexRDD[VD]

  def minus (other: VertexRDD[VD]): LineageVertexRDD[VD] = { null }


  def persist(newLevel: StorageLevel): this.type

  private[graphx] def shipVertexIds(): RDD[(PartitionID, Array[VertexId])]

  private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])]

  private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel): LineageVertexRDD[VD]

  private[graphx] def withPartitionsRDD[VD2: ClassTag](partitionsRDD: LineageRDD[ShippableVertexPartition[VD2]]): LineageVertexRDD[VD2]

}

object LineageVertex {

}
