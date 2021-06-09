package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{GraphImpl, LineageGraphImpl}

import scala.reflect.ClassTag
import org.apache.spark.graphx.{Edge, EdgeRDD, EdgeTriplet, Graph, PartitionStrategy, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Customized version of Graph extended to store Graph lineage info
 * @param classTag$VD$0
 * @param classTag$ED$0
 * @tparam VD
 * @tparam ED
 */

abstract class LineageGraphRDD[VD: ClassTag, ED: ClassTag] protected () extends Graph[VD, ED] with LineageGraph[VD, ED] {

  override val vertices: VertexRDD[VD]
  override val edges: EdgeRDD[ED]
  override val triplets: RDD[EdgeTriplet[VD, ED]]
}

object LineageGraphRDD {


  def apply[VD: ClassTag, ED: ClassTag](
       vertices: RDD[(VertexId, VD)],
       edges: RDD[Edge[ED]],
       defaultVertexAttr: VD = null.asInstanceOf[VD],
       edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
       vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): LineageGraph[VD, ED] = {
    LineageGraphImpl(vertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  def fromEdgeTuples[VD: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): LineageGraph[VD, Int] =
  {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = LineageGraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
  }

}