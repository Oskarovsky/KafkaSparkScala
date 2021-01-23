name := "KafkaSpark"
version := "0.1"
scalaVersion := "2.11.12"
val projectName = "KafkaSparkScala"


val sparkVersion = "2.2.2"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.8"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.4.6"
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6-M4"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.32"