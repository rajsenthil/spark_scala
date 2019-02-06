package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object ActorDegreesOfSeparation {

  val logger = LoggerFactory.getLogger(getClass.getName)

  // Array[Int] - connections, Int - distance, String - colors (WHITE: unvisited,GRAY: Visiting, BLACK: Visited/Done)
  type BFSData = (Array[Int], Int, String)
  //Int - Hero id, BFSData - connection data
  type BFSNode = (Int, BFSData)

  type OptionMap = Map[Symbol, Any]

  var hitCounter:Option[LongAccumulator] = None

  var fromHeroId: Int = 5306
  var toHeroId: Int = 14

  def bfsNode(line: String) : BFSNode = {
    val actors = line.split("\\s+")
    val heroId = actors(0).toInt
//    val connections = actors.tail
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 to (actors.length - 1)) {
      connections += actors(connection).toInt
    }


    var color = "WHITE"
    var distance: Int = 9999
    if (fromHeroId == heroId) {
      color = "GRAY"
      distance = 0
    }
    (heroId, (connections.toArray, distance, color))
  }

  def bfsMap(node: BFSNode): Array[BFSNode] = {
    val heroId = node._1
    val data = node._2

    val connections: Array[Int] = data._1
    val distance = data._2
    var color = data._3

    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    //The GRAY nodes are ready for expansion.
    // Note that during the bfsNode, the fromHeroId is already set to GRAY
    // So the expansion always and should start from fromHeroId to toHeroId
    if (color == "GRAY") {
      for(connection <- connections) {
        val newHeroId = connection
        val newDist = distance+1
        val newColor = "GRAY"

        //If the newHeroId is the target hero id, then,
        //notify the accumulator by incrementing it
        if (toHeroId == newHeroId) {
          if (hitCounter.isDefined) hitCounter.get.add(1)
        }

        val newEntry: BFSNode = (newHeroId, (Array(), newDist, newColor))
        results += newEntry
      }
      color = "BLACK"
    }
    val thisEntry: BFSNode = (heroId, (connections, distance, color))
    results += thisEntry
    results.toArray
  }

  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {
    // Extract data that we are combining
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3

    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
    (edges.toArray, distance, color)
  }

  def main(args: Array[String]): Unit = {

    fromHeroId = args(0).toInt
    toHeroId = args(1).toInt

    println(s"Degree of separation from $fromHeroId to $toHeroId")

    val conf = new SparkConf().setAppName("Actor to actor degree of separation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val graphPath = getClass.getResource("/Marvel-graph.txt")
    val namesPath = getClass.getResource("/Marvel-names.txt")

    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    val actorGraphs = sc.textFile(graphPath.getPath)

    var bfsRdd = actorGraphs.map(bfsNode)

    var iteration:Int = 0
    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = bfsRdd.flatMap(bfsMap)

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      bfsRdd = mapped.reduceByKey(bfsReduce)
    }

  }
}
