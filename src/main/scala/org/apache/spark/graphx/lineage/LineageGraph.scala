package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{LineageEdgeRDDImpl, LineageVertexRDDImpl}
import org.apache.spark.graphx.{EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD

/**
 * Lineage trait for LineageGraph to be used while creating Graph during Graph transformation and actions
 */
// I think we need to change the types into T or Any ???

// Passing Record Id( It can server as Input, and output Id) as an argument to the LineageGraphRDD would be great
trait LineageGraph[VD, ED] extends Graph[VD, ED] {

  override val vertices: LineageVertexRDDImpl[VD]
  override val edges: LineageEdgeRDDImpl[ED, VD]
  val triplets: RDD[EdgeTriplet[VD, ED]]

}

object LineageGraph {

}
