package org.apache.spark.graphx.impl

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._
import org.apache.spark.graphx.lineage.LineageGraph
import org.apache.spark.graphx.util.BytecodeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class LineageGraphImpl[VD: ClassTag, ED: ClassTag] (
       @transient val vertices: VertexRDD[VD],
       @transient val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends LineageGraph[VD, ED]{

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  @transient override val edges: EdgeRDDImpl[ED, VD] = replicatedVertexView.edges

  /** Return an RDD that brings edges together with their source and destination vertices. */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    replicatedVertexView.upgrade(vertices, true, true)
    replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def persist(newLevel: StorageLevel): LineageGraphImpl[VD, ED] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  override def cache(): LineageGraphImpl[VD, ED] = {
    vertices.cache()
    replicatedVertexView.edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq.empty
    }
  }

  override def unpersist(blocking: Boolean = true): LineageGraphImpl[VD, ED] = {
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean = true): LineageGraphImpl[VD, ED] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): LineageGraphImpl[VD, ED] = {
    partitionBy(partitionStrategy, edges.partitions.length)
  }

  override def partitionBy(
      partitionStrategy: PartitionStrategy, numPartitions: Int): LineageGraphImpl[VD, ED] = {
    val edTag = classTag[ED]
    val vdTag = classTag[VD]
    val newEdges = edges.withPartitionsRDD(edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
      (part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex({ (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
        iter.foreach { message =>
          val data = message._2
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true)).cache()
    LineageGraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    new LineageGraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
  }

  override def mapVertices[VD2: ClassTag]
  (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): LineageGraphImpl[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new LineageGraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      LineageGraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }

  override def mapEdges[ED2: ClassTag](
                                        f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new LineageGraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapTriplets[ED2: ClassTag](
                                           f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
                                           tripletFields: TripletFields): Graph[VD, ED2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new LineageGraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def subgraph(
       epred: EdgeTriplet[VD, ED] => Boolean = x => true,
       vpred: (VertexId, VD) => Boolean = (a, b) => true): LineageGraphImpl[VD, ED] = {
    vertices.cache()
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new LineageGraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def mask[VD2: ClassTag, ED2: ClassTag](
          other: Graph[VD2, ED2]): LineageGraphImpl[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new LineageGraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def groupEdges(merge: (ED, ED) => ED): LineageGraphImpl[VD, ED] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new LineageGraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateMessagesWithActiveSet[A: ClassTag](
          sendMsg: EdgeContext[VD, ED, A] => Unit,
          mergeMsg: (A, A) => A,
          tripletFields: TripletFields,
          activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]): VertexRDD[A] = {

    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, edgePartition) =>
        // Choose scan method
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            }
          case Some(EdgeDirection.Either) =>
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            }
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
    }).setName("GraphImpl.aggregateMessages - preAgg")

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, mergeMsg)
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
  (other: RDD[(VertexId, U)])
  (updateF: (VertexId, VD, Option[U]) => VD2)
  (implicit eq: VD =:= VD2 = null): LineageGraphImpl[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new LineageGraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      LineageGraphImpl(newVerts, replicatedVertexView.edges)
    }
  }

  /** Test whether the closure accesses the attribute with name `attrName`. */
  private def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }

}

object LineageGraphImpl {
  def apply[VD: ClassTag, ED: ClassTag](
       edges: RDD[Edge[ED]],
       defaultVertexAttr: VD,
       edgeStorageLevel: StorageLevel,
       vertexStorageLevel: StorageLevel): LineageGraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /**
   * Create a graph from EdgePartitions, setting referenced vertices to `defaultVertexAttr`.
   */
  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): LineageGraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdgePartitions(edgePartitions), defaultVertexAttr, edgeStorageLevel,
      vertexStorageLevel)
  }

  /**
   * Create a graph from vertices and edges, setting missing vertices to `defaultVertexAttr`.
   */
  def apply[VD: ClassTag, ED: ClassTag](
     vertices: RDD[(VertexId, VD)],
     edges: RDD[Edge[ED]],
     defaultVertexAttr: VD,
     edgeStorageLevel: StorageLevel,
     vertexStorageLevel: StorageLevel): LineageGraphImpl[VD, ED] = {
    val edgeRDD = EdgeRDD.fromEdges(edges)(classTag[ED], classTag[VD])
      .withTargetStorageLevel(edgeStorageLevel)
    val vertexRDD = VertexRDD(vertices, edgeRDD, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    LineageGraphImpl(vertexRDD, edgeRDD)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with arbitrary replicated vertices. The
   * VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def apply[VD: ClassTag, ED: ClassTag](
       vertices: VertexRDD[VD],
       edges: EdgeRDD[ED]): LineageGraphImpl[VD, ED] = {

    vertices.cache()

    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges.asInstanceOf[EdgeRDDImpl[ED, _]]
      .mapEdgePartitions((pid, part) => part.withoutVertexAttributes[VD])
      .cache()

    LineageGraphImpl.fromExistingRDDs(vertices, newEdges)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with the same replicated vertex type as the
   * vertices. The VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): LineageGraphImpl[VD, ED] = {
    new LineageGraphImpl(vertices, new ReplicatedVertexView(edges.asInstanceOf[EdgeRDDImpl[ED, VD]]))
  }

  /**
   * Create a graph from an EdgeRDD with the correct vertex type, setting missing vertices to
   * `defaultVertexAttr`. The vertices will have the same number of partitions as the EdgeRDD.
   */
  private def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
       edges: EdgeRDDImpl[ED, VD],
       defaultVertexAttr: VD,
       edgeStorageLevel: StorageLevel,
       vertexStorageLevel: StorageLevel): LineageGraphImpl[VD, ED] = {


    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      VertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
        .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }
}