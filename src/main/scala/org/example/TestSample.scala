package org.example

import org.apache.spark.graphx.{Edge, LineageContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lineage.LineageGraphRDD

object TestSample {
  def main(args: Array[String]): Unit = {

    //Create conf object

    val conf = new SparkConf()
      .setAppName("WordCount").setMaster("local[*]")
    //create spark context object
    val lc = new LineageContext(new SparkContext(conf))

    val verArray = Array(
      (1L, ("Philadelphia", 1580863)),
      (2L, ("Baltimore", 620961)),
      (3L, ("Harrisburg", 49528)),
      (4L, ("Wilmington", 70851)),
      (5L, ("New York", 8175133)),
      (6L, ("Scranton", 76089)))

    val edgeArray = Array(
      Edge(2L, 3L, 113),
      Edge(2L, 4L, 106),
      Edge(3L, 4L, 128),
      Edge(3L, 5L, 248),
      Edge(3L, 6L, 162),
      Edge(4L, 1L, 39),
      Edge(1L, 6L, 168),
      Edge(1L, 5L, 130),
      Edge(5L, 6L, 159))


    val verRDD = lc.parallelize(verArray)
    val edgeRDD = lc.parallelize(edgeArray)

    val graph = LineageGraphRDD(verRDD, edgeRDD)
    //cluster running


    graph.vertices.filter {
      case (id, (city, population)) => population > 50000
    }.collect.foreach {
      case (id, (city, population)) =>
        println(s"The population of $city is $population")
    }
    System.out.println("Finding Triplets:")
    for (triplet <- graph.triplets.collect) {
      println(s"""The distance between ${triplet.srcAttr._1} and
    ${triplet.dstAttr._1} is ${triplet.attr} kilometers""")
    }

    println("Filtration by edges")

    graph.edges.filter
    {
      case Edge(city1, city2, distance) => distance < 150
    }.collect.foreach {
      case Edge(city1, city2, distance) =>
        println(s"The distance between $city1 and $city2 is $distance")
    }
  }
}
