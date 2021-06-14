package org.apache.spark.graphx.impl

import org.apache.spark.graphx.lineage.{LineageEdge, LineageEdgeRDD}
import org.apache.spark.{HashPartitioner, OneToOneDependency}
import org.apache.spark.graphx.{Edge, EdgeRDD, EdgeTriplet, LineageContext, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class LineageEdgeRDDImpl[ED: ClassTag, VD: ClassTag] private[graphx] (
    @transient val lineageContext: LineageContext,
    @transient override val partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends LineageEdgeRDD[ED](lineageContext, List(new OneToOneDependency(partitionsRDD))) with LineageEdge[ED] {

  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }
  setName("EdgeRDD")

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * `PartitionID`s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  /**
   * Persists the edge partitions at the specified storage level, ignoring any existing target
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
   * Persists the edge partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY.
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
    firstParent[(PartitionID, EdgePartition[ED, VD])].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    partitionsRDD.getCheckpointFile
  }

  /** The number of edges in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_._2.size.toLong).reduce(_ + _)
  }

  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): LineageEdgeRDDImpl[ED2, VD] =
    mapEdgePartitions((pid, part) => part.map(f))

  override def reverse: LineageEdgeRDDImpl[ED, VD] = mapEdgePartitions((pid, part) => part.reverse)

  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): LineageEdgeRDDImpl[ED, VD] = {
    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): LineageEdgeRDDImpl[ED3, VD] = {
    val ed2Tag = classTag[ED2]
    val ed3Tag = classTag[ED3]
    this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
    })
  }

  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
   f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): LineageEdgeRDDImpl[ED2, VD2] = {
    this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  private[graphx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
   partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): LineageEdgeRDDImpl[ED2, VD2] = {
    new LineageEdgeRDDImpl(lineageContext, partitionsRDD, this.targetStorageLevel)
  }

  override private[graphx] def withTargetStorageLevel(
   targetStorageLevel: StorageLevel): LineageEdgeRDDImpl[ED, VD] = {
    new LineageEdgeRDDImpl(lineageContext, this.partitionsRDD, targetStorageLevel)
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: LineageEdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): LineageEdgeRDDImpl[ED3, VD] = {
    val ed2Tag = classTag[ED2]
    val ed3Tag = classTag[ED3]
    this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
    })
  }
}
