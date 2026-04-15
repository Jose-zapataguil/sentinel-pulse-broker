ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"


val pekkoVersion = "1.4.0"
val pekkoGrpcVersion = "1.2.0"
val logbackVersion = "1.5.32"

lazy val root = (project in file("."))
  .enablePlugins(org.apache.pekko.grpc.sbt.PekkoGrpcPlugin, JavaAppPackaging, DockerPlugin)
  .settings(
    name := "sentinel-pulse-broker",

    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-grpc-runtime" % pekkoGrpcVersion,
      "org.apache.pekko" %% "pekko-discovery" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,

      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,

    ),
    // Docker configuration (sbt-native-packager)
    dockerBaseImage := "eclipse-temurin:21.0.10_7-jre",
    Docker / packageName := "sentinel-pulse-broker",

    dockerUpdateLatest := true,

    dockerExposedPorts := Seq(8080),

    dockerEnvVars := Map(
      "BROKER_IP" -> "0.0.0.0",
      "BROKER_PORT" -> "8080"
    )
  )





