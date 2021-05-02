package com.oskarro.spark

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.oskarro.config.Constants
import com.oskarro.model.BusModel
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, expr, from_json, to_timestamp, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.language.postfixOps

object MainSparkConsumer {

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(
      Constants.busTopic01,
      "localhost:9092",
      "transport",
      "bus_spark2")
  }

  var stateMap: Map[String, BusModel] = Map[String, BusModel]()

  def readCurrentLocationOfVehicles(topic: String, kafkaServers: String,
                                    cassandraKeyspace: String, cassandraTable: String): Unit = {
    // Spark Session create
    val sparkSession = SparkSession
      .builder()
      .appName(Constants.appName)
      .config("spark.casandra.connection.host", "localhost")
      .master(Constants.masterValue)
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    // Read data from Kafka topic
    val inputDf = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .load()


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
      .withColumn("Time", to_timestamp($"Time"))
      .withColumn("Timestamp", to_timestamp($"Time"))


    def updateBusModel(input: BusModel): Unit = {
      if (Option(input.Time).isEmpty) {
        // TODO
      }
      if (stateMap.contains(input.VehicleNumber)) {
        println(s"Update model: ${input.VehicleNumber}")
        val maybeModel = stateMap(input.VehicleNumber)
        stateMap = stateMap + (input.VehicleNumber -> maybeModel)
      } else {
        println(s"Add model: ${input.VehicleNumber}")
        stateMap = stateMap + (input.VehicleNumber -> input)
      }
    }

    val writer = new ForeachWriter[Row] {

      def open(partitionId: Long, version: Long): Boolean = {
        // Open connection
        true
      }

      def process(record: Row): Unit = {
        // Write string to connection
        updateBusModel(
          BusModel(
            record.getAs("Lines"),
            record.getAs("Lon").toString.toDouble,
            record.getAs("VehicleNumber"),
            Timestamp.valueOf(record.getAs("Time").toString).toString,
            record.getAs("Lat").toString.toDouble,
            record.getAs("Brigade")
          )
        )
      }

      def close(errorOrNull: Throwable): Unit = {
        // Close the connection
      }
    }
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

    def calculateDistanceJson(Lines: String,
                              Lon: Double,
                              VehicleNumber: String,
                              Time: String,
                              Lat: Double,
                              Brigade: String): Double = {
      updateBusModel(BusModel(Lines, Lon, VehicleNumber, Time, Lat, Brigade))
      println(s"Check $VehicleNumber - ${stateMap.contains(VehicleNumber)}")
      if (stateMap.contains(VehicleNumber)) {
        val busModelPrev: BusModel = stateMap(VehicleNumber)
        val dystans = calculateDistance(busModelPrev, Lat, Lon)
        val tajm = calculateDuration(busModelPrev.Time, Time)
        val spid = calculateSpeed(dystans, tajm)
        println(s"DYSTANS:  $dystans")
//        println(s"CZAS:  $tajm")
        println(s"SPEED:  $spid")
        dystans
      } else {
        println("OJ BIDA BIDA...")
        12
      }
    }

    def calculateDurationFunc(VehicleNumber: String,
                              Time: String): Double = {
      if (stateMap.contains(VehicleNumber)) {
        val busModelPrev: BusModel = stateMap(VehicleNumber)
        val tajm = calculateDuration(busModelPrev.Time, Time)
        println(s"CZAS:  $tajm")
        tajm
      } else {
        0
      }
    }

    def calculateDistance(prevBus: BusModel, lat: Double, lon: Double): Double = {
      val latDistance = Math.toRadians(prevBus.Lat - lat)
      val lngDistance = Math.toRadians(prevBus.Lon - lon)
      val sinLat = Math.sin(latDistance / 2)
      val sinLng = Math.sin(lngDistance / 2)
      val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(prevBus.Lat)) *
          Math.cos(Math.toRadians(lat)) *
          sinLng * sinLng)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      (6371000 * c)
    }

    def calculateDuration(prevTime: String, currentTime: String): Double = {
      val firstTime: Timestamp = Timestamp.valueOf(prevTime)
      val secondTime: Timestamp = Timestamp.valueOf(currentTime)
      val diffInMillis = firstTime.getTime - secondTime.getTime
      TimeUnit.MILLISECONDS.convert(diffInMillis, TimeUnit.MILLISECONDS).abs
    }

    def calculateSpeed(distance: Double, duration: Double): Double = {
      val distanceKm: Double = distance/1000
      val durationHour: Double = duration/3600000
      (distanceKm/durationHour).abs
    }

    val calculateDistanceJsonUdf = udf(calculateDistanceJson _)
    val calculateDurationFuncUdf = udf(calculateDurationFunc _)
    val calculateSpeedUdf = udf(calculateSpeed _)


    val query = summaryWithIDs
      .withColumn("distance",
        calculateDistanceJsonUdf(
            col("Lines"),
            col("Lon"),
            col("VehicleNumber"),
            col("Time"),
            col("Lat"),
            col("Brigade")
        )
      )
      .withColumn("duration",
        calculateDurationFuncUdf(
          col("VehicleNumber"),
          col("Time")
        )
      )
      .withColumn("speed",
        calculateSpeedUdf($"distance", $"duration")
      )
      .writeStream
      .option("truncate", value = false)
//      .foreach(writer)
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        batchDF.show(20)
        batchDF.collect()
      }
      .start()
    query.awaitTermination()
  }

}
