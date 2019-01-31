package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

object RatingsCounter {
  def main(args: Array[String]) {
    // Set the log level to only print errors
    //    │    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val logger = LoggerFactory.getLogger(RatingsCounter.getClass.getName);
    logger.info("Creating spark context")

    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    logger.info("read file...")
    val path = getClass.getResource("/ml-latest-small/ratings.csv")
    val lines = sc.textFile(path.getPath)

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split(",")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

  }

}
