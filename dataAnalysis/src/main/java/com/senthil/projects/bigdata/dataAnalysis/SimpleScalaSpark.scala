package com.senthil.projects.bigdata.dataAnalysis

import java.text.SimpleDateFormat
import java.time.Period
import java.util.{Date, Scanner}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleScalaSpark {
  def main(args: Array[String]) {
    val sparkHome = sys.env.get("SPARK_HOME").getOrElse("../../../java/apache/spark-2.4.0-bin-hadoop2.7")
    val logFile = sparkHome + "/" + "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}