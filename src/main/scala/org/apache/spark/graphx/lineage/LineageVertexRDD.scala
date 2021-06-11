package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{LineageVertexRDDImpl, RoutingTablePartition, ShippableVertexPartition}
import org.apache.spark.graphx.{EdgeRDD, LineageContext, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark._

import scala.reflect.ClassTag
/**
 * Customized version of VertexRDD extended to store VertexRDD lineage info
 * Planning to use VertexRDD Id beccause it is unique throughout the graph
 * VertexRDD extended with Provenance Information
 */
abstract class LineageVertexRDD[VD](lc: LineageContext, deps: Seq[Dependency[_]])
  extends VertexRDD[VD] (lc.sparkContext, deps) with LineageVertex[VD] {

  override implicit protected def vdTag: ClassTag[VD] = ???

  override private[graphx] def partitionsRDD = ???

  override def lineageContext: LineageContext = lc

  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    firstParent[ShippableVertexPartition[VD]].iterator(part, context).next().iterator
  }

  override def filter(pred: Tuple2[VertexId, VD] => Boolean): LineageVertexRDDImpl[VD] =
    this.mapVertexPartitions(_.filter(Function.untupled(pred)))
}

object LineageVertexRDD {

  def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)]): LineageVertexRDDImpl[VD] = {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val vertexPartitions = vPartitioned.mapPartitions(
      iter => Iterator(ShippableVertexPartition(iter)),
      preservesPartitioning = true)
    new LineageVertexRDDImpl(vertexPartitions) // TODO: tap
  }

  def apply[VD: ClassTag](
    vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], defaultVal: VD): LineageVertexRDDImpl[VD] = {
    LineageVertexRDD(vertices, edges, defaultVal, (a, b) => a)  // TODO: tap
  }

  def apply[VD: ClassTag](
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], defaultVal: VD, mergeFunc: (VD, VD) => VD
    ): LineageVertexRDDImpl[VD] = {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(vertexIter, routingTable, defaultVal, mergeFunc))
    }
    new LineageVertexRDDImpl(vertexPartitions) // TODO: tap
  }


  def fromEdges[VD: ClassTag](
     edges: EdgeRDD[_], numPartitions: Int, defaultVal: VD): LineageVertexRDDImpl[VD] = {
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
      val routingTable =
        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
      Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
    }, preservesPartitioning = true)
    new LineageVertexRDDImpl(vertexPartitions)   // TODO: tap
  }

  private[graphx] def createRoutingTables(
     edges: EdgeRDD[_], vertexPartitioner: Partitioner): RDD[RoutingTablePartition] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid = edges.partitionsRDD.mapPartitions(_.flatMap(
      Function.tupled(RoutingTablePartition.edgePartitionToMsgs)))
      .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")

    val numEdgePartitions = edges.partitions.length
    vid2pid.partitionBy(vertexPartitioner).mapPartitions(
      iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
      preservesPartitioning = true)
  }
}
