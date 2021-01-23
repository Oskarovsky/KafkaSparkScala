package com.oskarro

import java.time.format.DateTimeFormatter

object Constants {
  val oskarTopic = "temat_oskar01"

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
}
