package com.senthil.projects.bigdata.dataAnalysis

import java.nio.charset.CodingErrorAction

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.{Codec, Source}

object PopularMovies {

  def parseLine(line: String) = {
    val fields = line.split(",")
    fields(1).toInt
  }

  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieMap: Map[Int, String] = Map()

    val path = getClass.getResource("/ml-latest-small/movies.csv")
    val source = Source.fromFile(path.getPath).getLines().drop(1)

    for(line <- source) {
      val fields = line.split(",")
      if (fields.length > 1) {
        movieMap += (fields(0).toInt -> fields(1))
      }
    }
    movieMap
  }

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass.getName)

    val conf = new SparkConf().setAppName("Popular Movies").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val movieNames = sc.broadcast(loadMovieNames)

    val path = getClass.getResource("/ml-latest-small/ratings.csv")
    val lines = sc.textFile(path.getPath).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val movies = lines.map(parseLine).map(x => (x,1)).reduceByKey((x,y) => x+y)
    val popularMovies = movies.map(x => (x._2, movieNames.value(x._1))).sortByKey().collect()
    for(movie <- popularMovies) {
      val movieName = movie._2
      val count = movie._1
      println(s"$movieName: $count")
    }
  }
}
