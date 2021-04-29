package com.oskarro.spark

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.config.Constants
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{atan2, cos, desc, expr, from_json, lag, pow, sin, sqrt, toRadians, to_timestamp, udf, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainSparkConsumer {

  case class BusModel(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(
      Constants.busTopic01,
      "localhost:9092",
      "transport",
      "bus_spark2")
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

    // Read data from Kafka topic
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .load()


//    inputDf = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val json = StructType(Array(
      StructField(name = "Lines", dataType = StringType, nullable = true),
      StructField(name = "Lon", dataType = StringType, nullable = true),
      StructField(name = "VehicleNumber", dataType = StringType, nullable = true),
      StructField(name = "Time", dataType = TimestampType, nullable = true),
      StructField(name = "Lat", dataType = StringType, nullable = true),
      StructField(name = "Brigade", dataType = StringType, nullable = true)
    ))

//    @Deprecated("StructType object has been replaced with newer version")
    val jsonSchema = new StructType()
      .add("Lines", "string")
      .add("Lon", "string")
      .add("VehicleNumber", "string")
      .add("Time", "timestamp")
      .add("Lat", "string")
      .add("Brigade", "string")

    val trafficStream = inputDf
      .withColumn("traffic", from_json($"value".cast(StringType), json))
      .selectExpr("traffic.*", "Partition", "Offset")

    val makeUUID = udf(() => Uuids.timeBased().toString)
    val summaryWithIDs = trafficStream
      .withColumn("Uuid", makeUUID())
//      .withColumn("Time", to_timestamp($"Time"))


    // for checking data frame schema
    val schema: StructType = summaryWithIDs.schema
    val schemaJson: String = summaryWithIDs.schema.prettyJson

    val query = summaryWithIDs
      .withColumn("avg1", expr("(Lon-Lines)/2"))
      .withColumn("avg2", expr("(Lines-Lon)/2"))
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        batchDF.show(6)
        batchDF.collect()
      }
      .start()
    query.awaitTermination()


    // TODO
    val w = org.apache.spark.sql.expressions.Window
      .partitionBy("Lines", "Brigade")
      .orderBy(desc("Time"))

/*    summaryWithIDs
      .where("Time is not null")
      .withColumn("prev_lat", lag("Lat", 1, 0).over(w))
      .withColumn("prev_long", lag("Lon", 1, 0).over(w))
      .withColumn("prev_time", lag("Time", 1).over(w))
      .withColumn("a", pow(sin(toRadians($"Lat" - $"prev_lat") / 2), 2) + cos(toRadians($"prev_lat")) * cos(toRadians($"Lat")) * pow(sin(toRadians($"Lon" - $"prev_long") / 2), 2))
      .withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)
      .withColumn("time_diff", unix_timestamp($"Time") - unix_timestamp($"prev_time"))
      .withColumn("speed", $"distance"/$"time_diff" * 3600)
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/
/*
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
  */
  }
}
