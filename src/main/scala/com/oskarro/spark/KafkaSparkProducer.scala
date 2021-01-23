package com.oskarro.spark

import com.oskarro.Constants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaSparkProducer {


  def writeToKafka(info: String, topic: String, props: Properties = Constants.properties, content: String): Unit = {

    // Send data on Kafka topic
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, content)
    producer.send(record)
    producer.close()
  }

}
