package org.apache.spark.graphx.impl

import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx.lineage.{LineageEdgeRDD, LineageRDD, LineageVertex, LineageVertexRDD}
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
     @transient val lineageContext: LineageContext,
     @transient val partitionsRDD: RDD[ShippableVertexPartition[VD]],
     val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) (implicit override protected val vdTag: ClassTag[VD])
  extends LineageVertexRDD[VD](lineageContext, List(new OneToOneDependency(partitionsRDD))) with LineageVertex[VD] {

  require(partitionsRDD.partitioner.isDefined)

  override def aggregateUsingIndex[VD2: ClassTag](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): LineageVertexRDD[VD2] = {
    val shuffled = messages.partitionBy(this.partitioner.get)
    val parts = partitionsRDD.zipPartitions(shuffled, preservesPartitioning = true) { (thisIter, msgIter) =>
      thisIter.map(_.aggregateUsingIndex(msgIter, reduceFunc))
    }
    this.withPartitionsRDD[VD2](parts)
  }

  override def reindex(): LineageVertexRDD[VD] = this.withPartitionsRDD(partitionsRDD.map(_.reindex()))


  setName("VertexRDD")

  /**
   * Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY.
   */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def diff(other: RDD[(VertexId, VD)]): LineageVertexRDD[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def diff(other: LineageVertexRDD[VD]): LineageVertexRDD[VD] = {
    val otherPartition = other match {
      case other: LineageVertexRDD[_] if this.partitioner == other.partitioner =>
        other.partitionsRDD
      case _ =>
        LineageVertexRDD(lineageContext,other.partitionBy(this.partitioner.get)).partitionsRDD
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

  override def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
                (f: (VertexId, VD, U) => VD2): LineageVertexRDD[VD2] = {
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

    override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: LineageVertexRDD[U]) (f: (VertexId, VD, U) => VD2): LineageVertexRDD[VD2] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions( other.partitionsRDD, preservesPartitioning = true)
    {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        val otherPart = otherIter.next()
        Iterator(thisPart.innerJoin(otherPart)(f))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }


  override def leftJoin[VD2: ClassTag, VD3: ClassTag] (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3) : LineageVertexRDD[VD3] = {
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

  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
    (other: LineageVertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): LineageVertexRDD[VD3] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }

  override def mapValues[VD2: ClassTag](f: VD => VD2): LineageVertexRDD[VD2] =
    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

  override def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): LineageVertexRDD[VD2] =
    this.mapVertexPartitions(_.map(f))

  override def mapVertexPartitions[VD2: ClassTag]
    (f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]) : LineageVertexRDD[VD2] = {

    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    this.withPartitionsRDD(newPartitionsRDD)

  }

  override def minus(other: RDD[(VertexId, VD)]): LineageVertexRDD[VD] = {
    minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def minus (other: LineageVertexRDD[VD]): LineageVertexRDD[VD] = {
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

  override private[graphx] def withPartitionsRDD[VD2: ClassTag](
     partitionsRDD: RDD[ShippableVertexPartition[VD2]]): LineageVertexRDD[VD2] = {
    new LineageVertexRDDImpl(lineageContext, partitionsRDD, this.targetStorageLevel)
  }

  override private[graphx] def shipVertexIds(): RDD[(PartitionID, Array[VertexId])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexIds()))
  }

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexAttributes(shipSrc, shipDst)))
  }

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel): LineageVertexRDD[VD] = {
    new LineageVertexRDDImpl(lineageContext, this.partitionsRDD, targetStorageLevel)
  }


  override def withEdges(edges: LineageEdgeRDD[_]): LineageVertexRDD[VD] = {
    val routingTables = LineageVertexRDD.createRoutingTables(edges, this.partitioner.get)
    val vertexPartitions = partitionsRDD.zipPartitions(routingTables, preservesPartitioning = true) {
      (partIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    this.withPartitionsRDD(vertexPartitions)
  }

  override private[graphx] def withPartitionsRDD[VD2: ClassTag](
   partitionsRDD: LineageRDD[ShippableVertexPartition[VD2]]): LineageVertexRDD[VD2] = {
    new LineageVertexRDDImpl(lineageContext, partitionsRDD, this.targetStorageLevel)
  }

}

object LineageVertexRDDImpl {

}