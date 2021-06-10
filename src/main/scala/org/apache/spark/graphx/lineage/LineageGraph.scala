package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

/**
 * Lineage trait for LineageGraph to be used while creating Graph during Graph transformation and actions
 */
// I think we need to change the types into T or Any ???
trait LineageGraph[VD, ED] extends Graph[VD, ED] {

  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]



}

object LineageGraph {

}
