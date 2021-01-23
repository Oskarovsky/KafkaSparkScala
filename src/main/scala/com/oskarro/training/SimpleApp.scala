package com.oskarro.training

import com.oskarro.Constants
import org.apache.spark.sql.SparkSession

object SimpleApp {

  val appName: String = "TrainingApp"
  val masterValue: String = "local[1]"
  val logFile = "/home/oskarro/Developer/BigData/xxx/abc.txt" // Should be some file on your system


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(masterValue)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", Constants.oskarTopic)
      .load()

    val query = inputDf
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()


  }
}
