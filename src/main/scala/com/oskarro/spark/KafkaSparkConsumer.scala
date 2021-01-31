package com.oskarro.spark

import akka.actor.ProviderSelection.Cluster
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.Constants
import com.oskarro.training.SimpleApp.{appName, masterValue}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_json, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql._

import java.util.Properties

object KafkaSparkConsumer {

  val appName: String = "TrainingApp"
  val masterValue: String = "local[1]"
  val logFile = "/home/oskarro/Developer/BigData/xxx/abc.txt" // Should be some file on your system

  case class BusStream(Lines: String, Lon: String, VehicleNumber: String, Time: String, Lat: String, Brigade: String)
  case class BusStreamData(Lines: Double, Lon: Double, VehicleNumber: Double, Time: String, Lat: Double, Brigade: Double)

  def main(args: Array[String]): Unit = {
    readKafkaMessage("sdsd", Constants.properties)
  }

  def readKafkaMessage(topic: String, properties: Properties): Unit = {
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

    val summaryWithIDs = trafficStream.withColumn("uuid", makeUUID())

    val query = summaryWithIDs
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to cassandra...")
        /*        batchDF.write
                  .cassandraFormat("bus_stream", "stuff") // table, keyspace
                  .mode("append")
                  .save()*/
      }
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }

}
