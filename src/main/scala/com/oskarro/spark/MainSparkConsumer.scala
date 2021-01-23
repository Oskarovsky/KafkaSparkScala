package com.oskarro.spark

import com.oskarro.Constants

import java.util.Properties

object MainSparkConsumer {

  val apiKey: String = "3b168711-aefd-4825-973a-4e1526c6ce93"
  val resourceID: String = "2e5503e-927d-4ad3-9500-4ab9e55deb59"

  case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles("temat_oskar01", Constants.properties)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
  }

}

