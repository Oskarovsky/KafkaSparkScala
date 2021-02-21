package com.oskarro.enums

import com.oskarro.enums

object VehicleType extends Enumeration {

  type VehicleType = Value

  // Assigning values
  val bus: enums.VehicleType.Value = Value(1, "bus")
  val tram: enums.VehicleType.Value = Value(2, "tram")

}
