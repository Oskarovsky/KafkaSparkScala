package com.oskarro.config

import java.time.format.DateTimeFormatter
import java.util.Properties

object Constants {

  var appName: String = "KafkaSparkApp"
  var masterValue: String = "local[1]"
  var logFile = "/home/oskarro/Developer/BigData/xxx/abc.txt" // Should be some file on your system

  var busTopic01 = "busTopic01"

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  val properties: Properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var apiKey: String = "3b168711-aefd-4825-973a-4e1526c6ce93"
  var resourceID: String = "2e5503e-927d-4ad3-9500-4ab9e55deb59"
}
