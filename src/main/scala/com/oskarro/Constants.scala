package com.oskarro

import java.time.format.DateTimeFormatter
import java.util.Properties

object Constants {
  val oskarTopic = "temat_oskar01"

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  val properties: Properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
}
