package com.oskarro.model

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class BusRideModel(line: Int,
                        distance: Double,
                        speed: Double,
                        vehicleNumber: String,
                        timestamp: Timestamp,
                        brigade: Double,
                        busModels: Seq[BusModel])
