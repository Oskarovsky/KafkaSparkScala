package com.oskarro.training

import com.oskarro.Constants
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}



object SimpleApp {

  val appName: String = "TrainingApp"
  val masterValue: String = "local[1]"
  val logFile = "/home/oskarro/Developer/BigData/xxx/abc.txt" // Should be some file on your system

  case class BusStream(Lines: String, Lon: String, VehicleNumber: String, Time: String, Lat: String, Brigade: String)


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(masterValue)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val jsonSchema = new StructType()
      .add("Lines", "string")
      .add("Lon", "string")
      .add("VehicleNumber", "string")
      .add("Time", "string")
      .add("Lat", "string")
      .add("Brigade", "string")

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", Constants.oskarTopic)
      .load()

    val rawDF = inputDf.selectExpr("CAST(value AS STRING)").as[String]

    val trafficStream = inputDf
      .withColumn("traffic", from_json($"value".cast(StringType), jsonSchema))
      .selectExpr("traffic.*", "partition", "offset")

    // value --> "123,kd,315,ssia3,31
    val expandedDF = rawDF.map(row => row.split(","))
      .map(row => BusStream(
        row(1),
        row(2),
        row(3),
        row(4),
        row(5),
        row(6)
      ))


    val query = trafficStream
      .writeStream
      .outputMode("update")
      .format("console")

    query.start() .awaitTermination()


  }
}
