package com.oskarro.spark

import com.oskarro.services.KafkaService
import com.oskarro.config.Constants.{apiKey, resourceID}
import com.oskarro.config.Constants
import com.oskarro.enums.VehicleType
import com.oskarro.enums.VehicleType.VehicleType
import com.oskarro.model.BusModel
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.concurrent.duration.DurationInt

object MainSparkProducer {

  val kafkaService = new KafkaService()

  def main(args: Array[String]): Unit = {
    val system = akka.actor.ActorSystem("system")
    import system.dispatcher
    /*    system.scheduler.schedule(2 seconds, 10 seconds) {
      produceCurrentLocationOfVehicles(VehicleType.bus)
    }*/
    system.scheduler.schedule(2 seconds, 6 seconds) {
      produceCurrentLocationOfVehicles(VehicleType.tram)
    }
  }


  def produceCurrentLocationOfVehicles(vehicleType: VehicleType): Unit = {
    if (!VehicleType.values.toList.contains(vehicleType)) {
      throw new RuntimeException("There are API endpoints only for trams and buses")
    }

    val now = Calendar.getInstance().getTime
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
    println(s"[Timestamp - ${dataFormat.format(now)}] JSON Data for $vehicleType parsing started.")
    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> resourceID,
        "apikey" -> apiKey,
        "type" -> vehicleType.id.toString))

    val jsonObjectFromString = Json.parse(req.text)
    val response = jsonObjectFromString \ "result"

    implicit val formats: DefaultFormats.type = DefaultFormats
    val vehicleList = parse(response.get.toString()).extract[List[BusModel]]
    val infoAboutProcess: String = s"[PROCESS: $vehicleType localization]"
    vehicleList foreach {
      veh =>
          kafkaService.writeToKafka(infoAboutProcess, Constants.busTopic01, Constants.properties, write(veh))
    }

  }

}
