package com.sentinelpulse.broker

import com.sentinelpulse.broker.config.{BrokerParameters, ProducerParameters}
import com.sentinelpulse.broker.core.BrokerGuardian
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.ActorSystem

import scala.jdk.DurationConverters.*

object Broker:

  def main(args: Array[String]): Unit = {

    val defaultConf = ConfigFactory.defaultApplication()
    val conf = ConfigFactory.load()
    val finalConf = conf.withFallback(defaultConf)

    val ip = conf.getString("broker.ip")
    val port = conf.getInt("broker.port")
    val nOfActors = conf.getInt("broker.actors")
    val storageMode = conf.getString("broker.mode")
    val producerTimeout = conf.getDuration("broker.producer.timeout").toScala
    val producerParallelism = getOptionalInt(conf, "broker.producer.parallelism")

    val producerParameters = ProducerParameters(producerTimeout, producerParallelism)

    val brokerParameters = BrokerParameters(ip, port, nOfActors, storageMode,producerParameters)

    val system = ActorSystem[Nothing](BrokerGuardian(brokerParameters), "broker-system", finalConf)

  }
  
  
  def getOptionalInt(conf: Config, path: String): Option[Int] =
    if conf.hasPath(path) then
      Some(conf.getInt(path))
    else None

end Broker
