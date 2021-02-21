package com.oskarro.spark

import com.oskarro.Constants
import com.oskarro.Constants.{apiKey, resourceID}
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.concurrent.duration.DurationInt

object MainSparkProducer {

  def main(args: Array[String]): Unit = {
    val system = akka.actor.ActorSystem("system")
    import system.dispatcher
    /*    system.scheduler.schedule(2 seconds, 10 seconds) {
      produceCurrentLocationOfVehicles("bus")
    }*/
    system.scheduler.schedule(2 seconds, 6 seconds) {
      produceCurrentLocationOfVehicles("tram")
    }
  }

  def writeToKafka(info: String, topic: String, props: Properties = Constants.properties, content: String): Unit = {
    // Send data on Kafka topic
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, content)
    producer.send(record)
    producer.close()
  }

  def produceCurrentLocationOfVehicles(vehicleType: String): Unit = {
    case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

    var vehicleTypeNumber: String = ""
    if (vehicleType == "bus") {
      vehicleTypeNumber = "1"
    } else if (vehicleType == "tram") {
      vehicleTypeNumber = "2"
    } else {
      throw new RuntimeException("There are API endpoints only for trams and buses")
    }

    val now = Calendar.getInstance().getTime
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
    println(s"[Timestamp - ${dataFormat.format(now)}] JSON Data for $vehicleType parsing started.")
    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> resourceID,
        "apikey" -> apiKey,
        "type" -> vehicleTypeNumber))

    val jsonObjectFromString = Json.parse(req.text)
    val response = jsonObjectFromString \ "result"

    implicit val formats: DefaultFormats.type = DefaultFormats
    val vehicleList = parse(response.get.toString()).extract[List[BusStream]]
    val infoAboutProcess: String = s"[PROCESS: $vehicleType localization]"
    vehicleList foreach {
      veh =>
          writeToKafka(infoAboutProcess, "temat_oskar01", Constants.properties, write(veh))
    }

  }

}
