package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{GraphImpl, LineageGraphImpl}
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeRDD, EdgeTriplet, Graph, PartitionID, PartitionStrategy, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

trait LineageGraph[VD, ED] extends Graph[VD, ED] {

  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]

}

object LineageGraph {

}
