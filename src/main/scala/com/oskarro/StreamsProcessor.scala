package com.oskarro

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.util.Properties

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    val props: Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    val producer = new KafkaProducer[String, String](props)

    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](Constants.oskarTopic, i.toString, "My Site is sparkbyexamples.com " + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    } catch{
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }


    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", Constants.oskarTopic)
      .option("startingOffsets", "earliest")
      .load()



/*    df.selectExpr()
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "temat_oskar01")
      .option("checkpointLocation", "/home/oskarro/Developer/BigData/xxx")
      .start()
      .awaitTermination()*/
  }
}
