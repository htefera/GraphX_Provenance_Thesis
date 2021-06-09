package org.apache.spark.graphx.lineage

import org.apache.spark.Partition
import org.apache.spark.graphx.impl.{EdgePartition, ShippableVertexPartition}
import org.apache.spark.graphx.{EdgeRDD, LineageContext, PartitionID, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait LineageEdge[ED] extends EdgeRDD[ED] {

  implicit protected def vdTag: ClassTag[ED]
  private[graphx] def partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])] forSome { type VD }
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions


  @transient def lineageContext: LineageContext

}
