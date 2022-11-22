package org.example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, flatspec}



class RateCountSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter{
  implicit var spark: SparkSession = _
  val path = "src/test/resources/movie_metadata.csv"
  var dataFrame: DataFrame = null
  before {
    spark = SparkSession
      .builder()
      .appName("RateCount")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    dataFrame = spark.read.option("header",value = true).csv(path).toDF().na.drop(Seq("imdb_score")).withColumn("imdb_score",col("imdb_score").cast("double"))
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
  it should "rateCountAvgRating work for wordCount" in {
    rateCountAvgRating.rateCount(dataFrame).take(3).mkString("Array(", ", ", ")") shouldBe "Array([Eraser ,6.1], [Hollow Man ,5.7], [The Twilight Saga: New Moon ,4.6])"
  }
  it should "rateCountStandardDeviation work for wordCount" in {
    rateCountStandardDeviation.rateCount(dataFrame).take(3).mkString("Array(", ", ", ")") shouldBe "Array([Eraser ,0.0], [Hollow Man ,0.0], [The Twilight Saga: New Moon ,0.0])"
  }
}
