package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Direction.Direction
import org.apache.spark.graphx.lineage._
import org.apache.spark.internal.Logging

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag


/**
 * Customized version of SparkContext used to capture and store lineage info, and enable/disable lineage capturing
 * If we use Titian Lineage Context as it is we need to copy all the dependencies because Titian Lineage uses lots of dependencies
 */

class LineageContext(@transient val sparkContext: SparkContext) extends Logging with Serializable {

  private var captureLineage: Boolean = false

  private var currentLineagePosition: Option[LineageVertex[_]] = None

  private var lastLineagePosition: Option[LineageVertex[_]] = None

  private var lastLineageSeen: Option[LineageVertex[_]] = None

  def parallelize[T:ClassTag](seq: Seq[T], numSlices: Int = sparkContext.defaultParallelism): Lineage[T]=
  {
    val rdd = new ParallelCollectionLRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())

    if(isLineageActive) {
      //rdd.tapRight()   // TODO
      rdd
    } else {
      rdd
    }
  }

  def getCurrentLineagePosition = currentLineagePosition

  def getLastLineageSeen = lastLineageSeen

  def setCurrentLineagePosition(initialRDD: Option[LineageVertex[_]]) = {
    // Cleaning up
    prevLineagePosition.clear()
    lastOperation = None

    if(lastLineagePosition.isDefined && lastLineagePosition.get != initialRDD.get) {
      currentLineagePosition = lastLineagePosition

      // We are starting from the middle, fill the stack with prev positions
      if(currentLineagePosition.get != initialRDD.get) {
        prevLineagePosition.pushAll(search(List(currentLineagePosition.get), initialRDD.get))
      }
    }
    currentLineagePosition = initialRDD
    lastLineageSeen = currentLineagePosition
  }



  def search(path: List[LineageVertex[_]], initialRDD: LineageVertex[_]): List[LineageVertex[_]] = {
    path.map(rdd =>
      if(rdd.id == initialRDD.id) return path
    )
    path.foreach(p => {
      p.dependencies.map(dep => {
        val tmp = search(List(dep.rdd.asInstanceOf[LineageVertex[_]]), initialRDD)
        if (!tmp.isEmpty) return p :: tmp
      })
    })
    return Nil
  }

  def setLastLineagePosition(finalRDD: Option[LineageVertex[_]]) = lastLineagePosition = finalRDD

  def getlastOperation = lastOperation

  private[spark] var prevLineagePosition = new mutable.Stack[LineageVertex[_]]()

  private[spark] var lastOperation: Option[Direction] = None

  def isLineageActive: Boolean = captureLineage

  def setCaptureLineage(newLineage: Boolean) = {
    if(newLineage == false && captureLineage == true) {
      getLineage(lastLineagePosition.get)
    }
    captureLineage = newLineage
  }


/** we should able to trace back until the input data */
  def getBackward(path: Int = 0) = {
    // TODO
  }

  def getForward(): LineageVertex[((Int, _), Any)] = {
    // TODO
    null
  }

  /** GetLineage returns position of the current Lineage.
   *  VertexRDD and EdgeRDD Lineages should be included or called from Graph Lineage
   * Therefore make getLineage method to return position of GraphLineageRDD instead of implementing
   * an overloaded getLineage method for each abstractions
  */

  def getLineage(rdd: LineageVertex[_]) = {

  }

  def getLineage(rdd:LineageGraph[_,_]): Unit =
  {

  }

  def getLineage(rdd: LineageEdge[_]) = {

  }

}

object Direction extends Enumeration {
  type Direction = Value
  val FORWARD, BACKWARD = Value
}
