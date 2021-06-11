package org.apache.spark.graphx.impl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


private[impl]
class LineageReplicatedVertexView[VD: ClassTag, ED: ClassTag](
      var edges: LineageEdgeRDDImpl[ED, VD],
      var hasSrcId: Boolean = false,
      var hasDstId: Boolean = false) {

  /**
   * Return a new `ReplicatedVertexView` with the specified `EdgeRDD`, which must have the same
   * shipping level.
   */
  def withEdges[VD2: ClassTag, ED2: ClassTag](
       _edges: LineageEdgeRDDImpl[ED2, VD2]): LineageReplicatedVertexView[VD2, ED2] = {
    new LineageReplicatedVertexView(_edges, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where edges are reversed and shipping levels are swapped to
   * match.
   */
  def reverse(): LineageReplicatedVertexView[VD, ED] = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new LineageReplicatedVertexView(newEdges, hasDstId, hasSrcId)
  }

  /**
   * Upgrade the shipping level in-place to the specified levels by shipping vertex attributes from
   * `vertices`. This operation modifies the `ReplicatedVertexView`, and callers can access `edges`
   * afterwards to obtain the upgraded view.
   */
  def upgrade(vertices: LineageVertexRDDImpl[VD], includeSrc: Boolean, includeDst: Boolean) {
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  /**
   * Return a new `ReplicatedVertexView` where the `activeSet` in each edge partition contains only
   * vertex ids present in `actives`. This ships a vertex id to all edge partitions where it is
   * referenced, ignoring the attribute shipping level.
   */
  def withActiveSet(actives: LineageVertexRDDImpl[VD]): LineageReplicatedVertexView[VD, ED] = {
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })
    new LineageReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where vertex attributes in edge partition are updated using
   * `updates`. This ships a vertex attribute only to the edge partitions where it is in the
   * position(s) specified by the attribute shipping level.
   */
  def updateVertices(updates: LineageVertexRDDImpl[VD]): LineageReplicatedVertexView[VD, ED] = {
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    new LineageReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
