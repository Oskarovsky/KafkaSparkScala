package com.oskarro.spark

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.config.Constants
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{atan2, cos, from_json, lag, pow, sin, sqrt, toRadians, to_timestamp, udf, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainSparkConsumer {

  case class BusModel(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(
      Constants.busTopic01,
      "localhost:9092",
      "wawa",
      "bus_stream_spark")
  }


  def readCurrentLocationOfVehicles(topic: String, kafkaServers: String,
                                    cassandraKeyspace: String, cassandraTable: String): Unit = {
    // Spark Session create
    val spark = SparkSession
      .builder()
      .appName(Constants.appName)
      .config("spark.casandra.connection.host", "localhost")
      .master(Constants.masterValue)
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

    // Read data from Kafka topic
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .load()

    val makeUUID = udf(() => Uuids.timeBased().toString)

    val trafficStream = inputDf
      .withColumn("traffic", from_json($"value".cast(StringType), jsonSchema))
      .selectExpr("traffic.*", "partition", "offset")

    val summaryWithIDs = trafficStream
      .withColumn("uuid", makeUUID())
      .withColumn("Time", to_timestamp($"Time"))


    // TODO
/*    val w = org.apache.spark.sql.expressions.Window
      .partitionBy("Lines", "Brigade")
      .orderBy("Time")

    summaryWithIDs
      .where("Time is not null and Lines != 121")
      .withColumn("prev_lat", lag("Lat", 1, 0))
      .withColumn("prev_long", lag("Lon", 1, 0))
      .withColumn("prev_time", lag("Time", 1))
      .withColumn("a", pow(sin(toRadians($"Lat" - $"prev_lat") / 2), 2) + cos(toRadians($"prev_lat")) * cos(toRadians($"Lat")) * pow(sin(toRadians($"Lon" - $"prev_long") / 2), 2))
      .withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)
      .withColumn("time_diff", unix_timestamp($"Time") - unix_timestamp($"prev_time"))
      .withColumn("speed", $"distance"/$"time_diff" * 3600)
      .writeStream
      .format("console")
      .start().awaitTermination()*/

      val query = summaryWithIDs
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
          println(s"Writing to cassandra $batchID")
          batchDF.write
            .cassandraFormat(cassandraTable, cassandraKeyspace)
            .mode("append")
            .save()
        }
        .outputMode("update")
//        .format("console")
        .start()

    query.awaitTermination()
    }
}
