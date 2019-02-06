package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object CustomerOrders {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass.getName)
    val conf = new SparkConf().setAppName("Customer Orders").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = getClass.getResource("/customer-orders.csv")
    val lines = sc.textFile(path.getPath)
    val customerOrders = lines.map(parseLine).reduceByKey((x,y) => x+y)
    val sortedByAmount = customerOrders.map(x => (x._2, x._1)).sortByKey()
    for(result <- sortedByAmount) {
      val id = result._2
      val amount = result._1
      val formattedAmount = f"$amount%.2f"
      println(s"$id: $formattedAmount")
    }
  }
}
