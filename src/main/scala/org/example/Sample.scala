package org.example

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}


object Sample {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("GraphX").setMaster("local")

    val sc= new SparkContext(conf)

    val vertexArray =sc.parallelize(Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))))
    val edgeArray = sc.parallelize(Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)))
    //after I add pararallelize
    val graph=  Graph(vertexArray,edgeArray).cache()







  }
}
