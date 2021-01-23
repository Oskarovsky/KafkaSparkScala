package com.oskarro

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Kafka-Spark-Scala")
      .master("local[*]")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(
      Seq("I like Spark", "Spark is awesome", "My first Spark job is working now and is counting down these words")
    )
    val filtered = data.filter(line => line.contains("awesome"))
    filtered.collect().foreach(print)


    val producer = new KafkaProducer[String, String](Constants.properties)

/*    try {
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
    }*/






    val schema = StructType


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", Constants.oskarTopic)
      .option("startingOffsets", "earliest")
      .load()


    Thread.sleep(300000)


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
