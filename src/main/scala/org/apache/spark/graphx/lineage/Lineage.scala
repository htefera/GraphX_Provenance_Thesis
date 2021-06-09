package org.apache.spark.graphx.lineage

import org.apache.spark.graphx.LineageContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Lineage[T] extends RDD[T]  {

  implicit def ttag: ClassTag[T]

  @transient def lineageContext: LineageContext
}

object Lineage {

}