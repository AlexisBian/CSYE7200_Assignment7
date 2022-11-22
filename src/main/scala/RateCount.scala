package org.example

import org.apache.spark.sql.functions.{avg, col, stddev_pop}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spire.compat.{fractional, numeric, ordering}


object RateCount extends App{
  val path = "src/test/resources/movie_metadata.csv"
  val spark: SparkSession = SparkSession
    .builder()
    .appName("RateCount")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.read.option("header",value = true)
  val dataFrame = spark.read.option("header",value = true).csv(path).toDF().na.drop(Seq("imdb_score")).withColumn("imdb_score",col("imdb_score").cast("double"))
  def printAvgRate(): Unit ={
    rateCountAvgRating.rateCount(dataFrame).take(20) foreach println
  }
  def printStandardDeviationRate(): Unit ={
    rateCountStandardDeviation.rateCount(dataFrame).take(20) foreach println
  }
  printStandardDeviationRate()
  printAvgRate()
}


object  rateCountAvgRating{
  def rateCount(lines: DataFrame): DataFrame = lines.groupBy("movie_title").agg(avg("imdb_score"))
}
object  rateCountStandardDeviation{
  def rateCount(lines: DataFrame): DataFrame = lines.groupBy("movie_title").agg(stddev_pop("imdb_score"))
}
