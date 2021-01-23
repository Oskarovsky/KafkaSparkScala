package com.oskarro

import org.apache.spark.sql.SparkSession

object StreamsProcessor {

  def main(args: Array[String]): Unit = {
    process()
  }

  def process(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka-Spark-Scala")
      .master("local[*]")
      .getOrCreate()

  }


}
