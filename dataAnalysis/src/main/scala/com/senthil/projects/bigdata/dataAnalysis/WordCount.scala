package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = getClass.getResource("/book.txt")
    val book = sc.textFile(path.getPath)
    val words = book.flatMap(x => x.split("\\W+"))
    val lowerCaseWords = words.map(x => x.toLowerCase)
//    val wordCounts = lowerCaseWords.countByValue();
    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => (x+y))
    val sortedWordCounts = wordCounts.map(x => (x._2, x._1)).sortByKey()
//    wordCounts.foreach(println)
    for(result <- sortedWordCounts) {
      val word = result._2
      val count = result._1
      println(s"$word: $count")
    }
  }
}
