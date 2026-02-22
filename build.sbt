ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"


val pekkoVersion = "1.4.0"
val logbackVersion = "1.5.25"
val pekkoGrpcVersion = "1.2.0"

lazy val root = (project in file("."))
  .enablePlugins(org.apache.pekko.grpc.sbt.PekkoGrpcPlugin)
  .settings(
    name := "sentinel-pulse-broker",

    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-grpc-runtime" % pekkoGrpcVersion,

      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
  )





