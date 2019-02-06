package com.senthil.projects.bigdata.dataAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.math.sqrt

object MovieSuggest {

  val log = LoggerFactory.getLogger(getClass.getName)

  def loadMovieNames(path: String) : Map[Int, String] = {
//    val lines = Source.fromFile(path).getLines().drop(1)
//    val lines = Source.fromInputStream(getClass.getResourceAsStream("./ml-latest-small/movies.csv")).getLines().drop(1)
    val lines = Source.fromFile("./ml-latest-small/movies.csv").getLines().drop(1)
    var movieNames: Map[Int, String] = Map()
    for(line <- lines) {
      val fields = line.split(",")
      if (fields.size > 1) movieNames += fields(0).toInt -> fields(1)
    }
    movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating)) // This is structure after join (see below) is called

  def filterDuplicates(userRatingPair: UserRatingPair): Boolean = {
    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 < movie2
  }

  type RatingPair = (Double, Double)

  def makeMovieAndRatingPair(userRatingPair: UserRatingPair)  = {
    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def createRatingRdd(sc: SparkContext, path: String) = {
//    val lines = sc.textFile(path).mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    val lines = sc.textFile("./ml-latest-small/ratings.csv").mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    lines.map(x=> x.split(",")).map(x => (x(0).toInt, new MovieRating(x(1).toInt, x(2).toDouble)))
  }

  def main(args: Array[String]): Unit = {
    val movieNamesFilePath = "/ml-latest-small/movies.csv"
    val ratgingsFilePath = "/ml-latest-small/ratings.csv"

    val movieNamesPath = getClass.getResource(movieNamesFilePath).getPath
    val ratingsPath = getClass.getResource(ratgingsFilePath).getPath

    log.info("Loading movie names...")
    val movieNameMap = loadMovieNames(movieNamesPath)
    log.info("Done.")

    val conf = new SparkConf().setAppName("Similar movies").setMaster("local[*]")
    val sc = new SparkContext(conf)

    log.info("Creating ratings rdd...")
    val rdd = createRatingRdd(sc, ratingsPath)
    log.info("Done.")

    val joinedRatings = rdd.join(rdd)
    val uniqueRatings = joinedRatings.filter(filterDuplicates)

    val movieAndRatingPair = uniqueRatings.map(makeMovieAndRatingPair)
    val movieRatingPair = movieAndRatingPair.groupByKey

    val moviePairSimilarities = movieRatingPair.mapValues(computeCosineSimilarity).cache()
/*

    val sorted = moviePairSimilarities.sortByKey()
    val savePath = getClass.getResource("/movie-sims")
    sorted.saveAsTextFile(savePath.getPath)
*/

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID:Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter( x =>
      {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      }
      )

      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)

      println("\nTop 10 similar movies for " + movieNameMap(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(movieNameMap(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }

  }
}
