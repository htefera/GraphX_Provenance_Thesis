package org.apache.spark.graphx.impl

import org.apache.spark.Partition
import org.apache.spark.graphx.lineage.{LineageRDD, LineageVertex, LineageVertexRDD}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

//VertexId: When you trace back from input to output , avoid any kind of conflict
// RDD-Id:
// srcId, destId: should be unique , consider this one: take care about the collision
// Consider twitter and Instagram Id: Overhead should be one problem
// Instagram one is preferable one

class LineageVertexRDDImpl[VD] private[graphx] (
     @transient val partitionsRDD: RDD[ShippableVertexPartition[VD]],
     val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) (implicit override protected val vdTag: ClassTag[VD]) extends LineageVertex[VD] {

  override def lineageContext: LineageContext = ???

  require(partitionsRDD.partitioner.isDefined)

  override def reindex(): LineageVertexRDDImpl[VD] = this.withPartitionsRDD(partitionsRDD.map(_.reindex()))

  override val partitioner = partitionsRDD.partitioner

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }
  setName("VertexRDD")

  /**
   * Persists the vertex partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /**
   * Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY.
   */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    partitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[ShippableVertexPartition[VD]].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    partitionsRDD.getCheckpointFile
  }

  /** The number of vertices in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_.size.toLong).reduce(_ + _)
  }

  override private[graphx] def mapVertexPartitions[VD2: ClassTag](
       f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]) : LineageVertexRDDImpl[VD2] = {

    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    this.withPartitionsRDD(newPartitionsRDD)

  }

  override def mapValues[VD2: ClassTag](f: VD => VD2): LineageVertexRDDImpl[VD2] =
    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

  override def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): LineageVertexRDDImpl[VD2] =
    this.mapVertexPartitions(_.map(f))

  def minus(other: LineageRDD[(VertexId, VD)]): LineageVertexRDDImpl[VD] = {
    minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  def minus (other: LineageVertex[VD]): LineageVertexRDDImpl[VD] = {
    other match {
      case other: LineageVertex[_] if this.partitioner == other.partitioner =>
        this.withPartitionsRDD[VD](
          partitionsRDD.zipPartitions(
            other.partitionsRDD, preservesPartitioning = true) {
            (thisIter, otherIter) =>
              val thisPart = thisIter.next()
              val otherPart = otherIter.next()
              Iterator(thisPart.minus(otherPart))
          })
      case _ =>
        this.withPartitionsRDD[VD](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.minus(msgs))
          }
        )
    }
  }

  override def diff(other: RDD[(VertexId, VD)]): LineageVertexRDDImpl[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  def diff(other: LineageVertexRDD[VD]): LineageVertexRDDImpl[VD] = {
    val otherPartition = other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        other.partitionsRDD
      case _ =>
        VertexRDD(other.partitionBy(this.partitioner.get)).partitionsRDD
    }
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      otherPartition, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }

  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
  (other: LineageVertex[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): LineageVertexRDDImpl[VD3] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }

  override def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3) : LineageVertexRDDImpl[VD3] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: LineageVertexRDD[VD2] if this.partitioner == other.partitioner =>
        leftZipJoin(other)(f)
      case _ =>
        this.withPartitionsRDD[VD3](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.leftJoin(msgs)(f))
          }
        )
    }
  }

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: LineageVertexRDDImpl[U])
     (f: (VertexId, VD, U) => VD2): LineageVertexRDDImpl[VD2] = {

    val newPartitionsRDD = partitionsRDD.zipPartitions( other.partitionsRDD, preservesPartitioning = true)
    {
      (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          val otherPart = otherIter.next()
          Iterator(thisPart.innerJoin(otherPart)(f))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }


  override def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (f: (VertexId, VD, U) => VD2): LineageVertexRDDImpl[VD2] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient innerZipJoin
    other match {
      case other: LineageVertexRDDImpl[U] if this.partitioner == other.partitioner =>
        this.innerZipJoin(other)(f)
      case _ =>
        this.withPartitionsRDD(
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.innerJoin(msgs)(f))
          }
        )
    }
  }

  override def aggregateUsingIndex[VD2: ClassTag](
     messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): LineageVertex[VD2] = {
    val shuffled = messages.partitionBy(this.partitioner.get)
    val parts = partitionsRDD.zipPartitions(shuffled, true) { (thisIter, msgIter) =>
      thisIter.map(_.aggregateUsingIndex(msgIter, reduceFunc))
    }
    this.withPartitionsRDD[VD2](parts)
  }

  override def reverseRoutingTables(): LineageVertexRDDImpl[VD] =
    this.mapVertexPartitions(vPart => vPart.withRoutingTable(vPart.routingTable.reverse))

  override def withEdges(edges: EdgeRDD[_]): LineageVertexRDDImpl[VD] = {
    val routingTables = LineageVertexRDD.createRoutingTables(edges, this.partitioner.get)
    val vertexPartitions = partitionsRDD.zipPartitions(routingTables, true) {
      (partIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    this.withPartitionsRDD(vertexPartitions)
  }

  override private[graphx] def withPartitionsRDD[VD2: ClassTag](
   partitionsRDD: RDD[ShippableVertexPartition[VD2]]): LineageVertexRDDImpl[VD2] = {
    new LineageVertexRDDImpl(partitionsRDD, this.targetStorageLevel)
  }

  override private[graphx] def withTargetStorageLevel(
             targetStorageLevel: StorageLevel): LineageVertexRDDImpl[VD] = {
    new LineageVertexRDDImpl(this.partitionsRDD, targetStorageLevel)
  }

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean):
    RDD[(PartitionID, VertexAttributeBlock[VD])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexAttributes(shipSrc, shipDst)))
  }

  override private[graphx] def shipVertexIds(): RDD[(PartitionID, Array[VertexId])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexIds()))
  }


  override def minus(other: RDD[(VertexId, VD)]): LineageVertexRDDImpl[VD] = ???

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = ???
}

object LineageVertexRDDImpl{
}