package com.oskarro.services

import com.oskarro.config.Constants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * KafkaService is a class that handle kafka process
 */

class KafkaService {

  /**
   * Send data to kafka topic with specific configurations
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   *
   * @param info is a string with basic information for client,
   * @param topic contains topic name,
   * @param props defines configuration,
   * @param content is sending on a topic
   */
  def writeToKafka(info: String, topic: String, props: Properties = Constants.properties, content: String): Unit = {
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, content)
    producer.send(record)
    producer.close()
  }

}
