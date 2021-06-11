package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder, LineageEdgeRDDImpl}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * Customized version of EdgeRDD extended to store EdgeRDD lineage info
 * Provenance information of EdgreRDD transformations
 * Copy will have some dependency problem. You have to copy all dependecny
 */

abstract class LineageEdgeRDD[ED](lc: LineageContext, deps: Seq[Dependency[_]]) extends EdgeRDD[ED](lc.sparkContext, deps)
    with LineageEdge[ED] {

  private[graphx] def partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])] forSome { type VD }

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, EdgePartition[ED, _])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  // TODO

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2]

  def reverse: EdgeRDD[ED]

  def innerJoin[ED2: ClassTag, ED3: ClassTag] (other: EdgeRDD[ED2])
    (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3]

  private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel): EdgeRDD[ED]
}

object LineageEdgeRDD{

  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): LineageEdgeRDDImpl[ED, VD] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    LineageEdgeRDD.fromEdgePartitions(edgePartitions)
  }

  private[graphx] def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
      edgePartitions: RDD[(Int, EdgePartition[ED, VD])]): LineageEdgeRDDImpl[ED, VD] = {
    new LineageEdgeRDDImpl(edgePartitions)
  }
}
