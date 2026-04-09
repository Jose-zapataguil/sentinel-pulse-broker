package com.sentinelpulse.broker

import com.sentinelpulse.broker.config.BrokerParameters
import com.sentinelpulse.broker.core.BrokerGuardian
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem

object Broker:

  def main(args: Array[String]): Unit = {

    val defaultConf = ConfigFactory.defaultApplication()
    val conf = ConfigFactory.load()
    val finalConf = conf.withFallback(defaultConf)

    val ip = conf.getString("broker.ip")
    val port = conf.getInt("broker.port")
    val nOfActors = conf.getInt("broker.actors")

    val brokerParameters = BrokerParameters(ip, port, nOfActors)

    val system = ActorSystem[Nothing](BrokerGuardian(brokerParameters), "broker-system", finalConf)

  }

end Broker
