package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.impl.{LineageEdgeRDDImpl, LineageVertexRDDImpl}
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeTriplet, Graph, GraphOps, PartitionID, PartitionStrategy, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Lineage trait for LineageGraph to be used while creating Graph during Graph transformation and actions
 */
// I think we need to change the types into T or Any ???

// Passing Record Id( It can server as Input, and output Id) as an argument to the LineageGraphRDD would be great
trait LineageGraph[VD, ED] extends Graph[VD, ED] {

  val vertices: LineageVertexRDD[VD] //should return LineageVertexRDD
  val edges: LineageEdgeRDDImpl[ED, VD] //should return LineageEdgeRDD
  val triplets: RDD[EdgeTriplet[VD, ED]] //???

  override def cache(): LineageGraphRDD[VD, ED]

  override def checkpoint(): Unit

  override def isCheckpointed: Boolean

  override def getCheckpointFiles: Seq[String]

  override def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): LineageGraphRDD[VD, ED]

  override def unpersist(blocking: Boolean = true): LineageGraphRDD[VD, ED]

  override def unpersistVertices(blocking: Boolean = true): LineageGraphRDD[VD, ED]

  override def partitionBy(partitionStrategy: PartitionStrategy): LineageGraphRDD[VD, ED]

  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): LineageGraphRDD[VD, ED]

  override def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): LineageGraphRDD[VD2, ED]

  override def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): LineageGraphRDD[VD, ED2] = {
    mapEdges((pid, iter) => iter.map(map))
  }

  override def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])
  : LineageGraphRDD[VD, ED2]

  override def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): LineageGraphRDD[VD, ED2] = {
    mapTriplets((pid, iter) => iter.map(map), TripletFields.All)
  }

  override def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2,
      tripletFields: TripletFields): LineageGraphRDD[VD, ED2] = {
    mapTriplets((pid, iter) => iter.map(map), tripletFields)
  }

  override def mapTriplets[ED2: ClassTag](
      map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): LineageGraphRDD[VD, ED2]

  override def reverse: LineageGraphRDD[VD, ED]

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
      vpred: (VertexId, VD) => Boolean = ((v, d) => true))
  : LineageGraphRDD[VD, ED]

  override def mask[VD2: ClassTag, ED2: ClassTag](other: Graph[VD2, ED2]): LineageGraphRDD[VD, ED]

  override def groupEdges(merge: (ED, ED) => ED): LineageGraphRDD[VD, ED]

  override def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
  : LineageVertexRDD[A] = {
    aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
  }

  private[graphx] def aggregateMessagesWithActiveSet[A:ClassTag](
         sendMsg: EdgeContext[VD, ED, A] => Unit,
         mergeMsg: (A, A) => A,
         tripletFields: TripletFields,
         activeSetOpt: Option[(LineageVertexRDD[_], EdgeDirection)])
  : LineageVertexRDD[A]

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
  : LineageGraphRDD[VD2, ED]

  //override val ops = new GraphOps(this)

}

object LineageGraph {

}
