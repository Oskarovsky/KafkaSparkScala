package com.oskarro.spark

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.config.Constants
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{from_json, to_timestamp, udf}
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

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .load()

      val trafficStream = inputDf
        .withColumn("traffic", from_json($"value".cast(StringType), jsonSchema))
        .selectExpr("traffic.*", "partition", "offset")

      val makeUUID = udf(() => Uuids.timeBased().toString)

      val summaryWithIDs = trafficStream
        .withColumn("uuid", makeUUID())
        .withColumn("Time", to_timestamp($"time"))


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
