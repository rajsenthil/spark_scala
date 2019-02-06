package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object MostPopularHero {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def mapHeroToCoOccurences(line: String) = {
    val list = line.split(" ").toList
    (list.head, list.tail)
  }

  def parseNames(line: String) : Option[(Int, String)] = {
    val lines = line.split('\"').toList
    if (lines.size > 1) {
      return Some(lines(0).trim.toInt, lines(1).replaceAll("\"", ""))
    } else {
      return None
    }
//    val idName = (lines(0).trim.toInt, lines(1).replaceAll("\"", ""))
//    idName
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Most Popular Hero")
    val sc = new SparkContext(conf)

    val graphPath = getClass.getResource("/Marvel-graph.txt")
    val namesPath = getClass.getResource("/Marvel-names.txt")

    val lines = sc.textFile(graphPath.getPath)
    val splitted = lines.map(mapHeroToCoOccurences)
    val mapHeroCoOccur = splitted.map(x => (x._1, x._2)).reduceByKey((x,y) => (x++y))
    val mapHeroCount = mapHeroCoOccur.map(x => (x._1.toInt, x._2.size))
    val flipped = mapHeroCount.map(x => (x._2, x._1))
    val mostPopHero = flipped.max()


    val nameLines = sc.textFile(namesPath.getPath)
    val namesMap = nameLines.flatMap(parseNames)

    val mostPopHeroName = namesMap.lookup(mostPopHero._2)
    logger.info(s"The most popular hero name is ${mostPopHeroName} with co-occurrences of ${mostPopHero._1}(${mostPopHero._2})")
    println(s"The most popular hero name is ${mostPopHeroName}(${mostPopHero._2}) with co-occurrences of ${mostPopHero._1}")

    val orderedPopHero = flipped.sortByKey(false).collect
    val orderedPopHeroNames = orderedPopHero.map(x => (x._1, namesMap.lookup(x._2)))

    logger.info(s"The popular hero order")
    for(order <- orderedPopHeroNames) {
      logger.info(s"Name: ${ order._2}, co-occurrences: ${order._1}")
    }
  }
}
