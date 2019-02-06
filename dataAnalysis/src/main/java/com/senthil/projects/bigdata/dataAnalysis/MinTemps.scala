package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.math.min

object MinTemps {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def parseLine(line: String) = {
    val fields = line.split(",")
    val id = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f/5.0f) + 32.0f
    (id, entryType, temp)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Mininum Temperatures by station")

    val conf = new SparkConf().setAppName("Minimum Temperature").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = getClass.getResource("/1800.csv")
    val lines = sc.textFile(path.getPath)
    val minTemps = lines.map(parseLine).filter(x=> x._2 == "TMIN")
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))

    val results = minTempsByStation.collect

    for (result <- results) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station min temperature: $formattedTemp")
    }
  }
}
