package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object FriendsByAge {
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numOfFriends = fields(3).toInt
    (age, numOfFriends)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting to calculate the Friends by age.")

    val conf = new SparkConf().setAppName("Friends By Age").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = getClass.getResource("/fakefriends.csv")
    logger.info("Path: {}",path)
    val lines = sc.textFile(path.getPath)
    val rdd = lines.map(parseLine)
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    val avgByAge = totalsByAge.mapValues(x => x._1/x._2)
    val results = avgByAge.collect()
    results.sorted.foreach(println)
  }
}
