package com.sentinelpulse.broker

import com.sentinelpulse.broker.config.{BrokerParameters, OverflowPolicy, ProducerParameters}
import com.sentinelpulse.broker.core.BrokerGuardian
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.ActorSystem
import org.slf4j.LoggerFactory
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*

object Broker:

  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {

    val defaultConf = ConfigFactory.defaultApplication()
    val conf = ConfigFactory.load()
    val finalConf = conf.withFallback(defaultConf)

    loadParameters(finalConf).flatMap(params => BrokerParameters.validate(params)) match {
      case Left(error) => logger.error("Invalid broker configuration: {}", error)
      case Right(validParams) => ActorSystem[Nothing](BrokerGuardian(validParams), "broker-system", finalConf)
    }

  }

  def loadParameters(conf: Config): Either[String, BrokerParameters] =

    val ip = conf.getString("broker.ip")
    val port = conf.getInt("broker.port")
    val nOfActors = conf.getInt("broker.actors")
    val storageMode = conf.getString("broker.mode")
    val producerTimeout = conf.getDuration("broker.producer.timeout").toScala
    val producerParallelism = getOptionalInt(conf, "broker.producer.parallelism")
    val maxQueueSize = conf.getInt("broker.max-messages-per-channel")
    val producerParameters = ProducerParameters(producerTimeout, producerParallelism)

    getOverflowPolicy(conf)
      .map(BrokerParameters(ip, port, nOfActors, storageMode, maxQueueSize, producerParameters, _))


  def getOverflowPolicy(conf: Config): Either[String, OverflowPolicy] = conf.getString("broker.overflowPolicy") match {
    case "drop-oldest" => Right(OverflowPolicy.DropOldest)
    case "reject" => Right(OverflowPolicy.Reject)
    case "backpressure" =>
      val timeout =
        if conf.hasPath("broker.backpressure-timeout") then
          conf.getDuration("broker.backpressure-timeout").toScala
        else 2.seconds
      Right(OverflowPolicy.Backpressure(timeout))
    case other => Left(s"Unknow overflow policy: $other")
  }

  def getOptionalInt(conf: Config, path: String): Option[Int] =
    if conf.hasPath(path) then
      Some(conf.getInt(path))
    else None

end Broker
