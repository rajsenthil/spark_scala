package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class TestSpec01 extends FlatSpec{

  val logger = LoggerFactory.getLogger(getClass.getName)

  "Joining the tuple of size n on to itself" should "have n times key occurring times" in {
    type tuple = (Int, (Int, Float))

    def createTuples(line: String) = {
      val fields = line.split(",")
      val userid = fields(0).toInt
      val movieid = fields(1).toInt
      val ratings = fields(2).toFloat

      new tuple(userid,(movieid, ratings))
    }

    val conf = new SparkConf().setMaster("local[1]").setAppName("Test join")
    val sc = new SparkContext(conf)
    val file = sc.textFile(getClass.getResource("/Data.csv").getPath)
    val rdd = file.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter} // Skip the header

    val userMovieRatings = rdd.map(createTuples)
    val joinedRatings = userMovieRatings.join(userMovieRatings).collect

    /*
    for(rating <- userMovieRatings.collect) {
      val mesg = s"User Id: ${rating._1}, movie id: ${rating._2._1}, rating: ${rating._2._2}"
      println(mesg)
      logger.info(mesg)
    }
    */

    var count = 0
    for(rating <- joinedRatings) {
      val mesg = s"User Id: ${rating._1}, movie id: ${rating._2._1}, rating: ${rating._2._2}"
      println(mesg)
      logger.info(mesg)
      count+=1
    }

    println(s"Count = $count")
    logger.info(s"Count = $count")
  }
}
