package com.oskarro.spark

import com.oskarro.Constants
import com.oskarro.Constants.{apiKey, resourceID}
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
          writeToKafka(infoAboutProcess, "temat_oskar01", Constants.properties, write(veh))
    }

  }

}
