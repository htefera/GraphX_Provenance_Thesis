package org.apache.spark.graphx.lineage

import org.apache.spark.{Dependency, Partition, TaskContext}
import org.apache.spark.graphx.LineageContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

class LineageRDD[T: ClassTag](
     @transient private var lc: LineageContext,
     @transient private var deps: Seq[Dependency[_]]
  ) extends RDD[T](lc.sparkContext, deps) with Lineage[T] {

  override def lineageContext: LineageContext = lc

  override def ttag: ClassTag[T] = classTag[T]


  override def getPartitions: Array[Partition] = firstParent[(Any, Any)].partitions

  override def compute(split: Partition, context: TaskContext) = {
    firstParent[T].iterator(split, context)//.map(r => r._1)
  }

}
