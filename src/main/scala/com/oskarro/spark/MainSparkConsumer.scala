package com.oskarro.spark

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.Constants
import com.oskarro.spark.KafkaSparkConsumer.{appName, masterValue}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_json, to_timestamp, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

import java.util.Properties

object MainSparkConsumer {

  val apiKey: String = "3b168711-aefd-4825-973a-4e1526c6ce93"
  val resourceID: String = "2e5503e-927d-4ad3-9500-4ab9e55deb59"

  case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles("temat_oskar01", Constants.properties)
  }


  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.casandra.connection.host", "localhost")
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
            .cassandraFormat("bus_stream", "metrics") // table, keyspace
            .mode("append")
            .save()
        }
        .outputMode("update")
//        .format("console")
        .start()

    query.awaitTermination()
    }


  /*    summaryWithIDs.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "stuff")
      .option("table", "bus_stream")
      .mode("append")
      .save()*/
}
