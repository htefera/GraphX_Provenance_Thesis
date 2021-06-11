package org.apache.spark.graphx.lineage

import org.apache.spark.Partition
import org.apache.spark.graphx.impl.{EdgePartition, ShippableVertexPartition}
import org.apache.spark.graphx.{EdgeRDD, LineageContext, PartitionID, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Lineage trait for LineageEdgeRDD to be used while creating EdgeRDD during EgdeRDD transformation and actions
 */

trait LineageEdge[ED] extends EdgeRDD[ED] {


}
