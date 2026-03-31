package com.sentinelpulse.broker

import com.sentinelpulse.broker.core.BrokerGuardian
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem

object Broker:

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseString("pekko.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())

    val system = ActorSystem[Nothing](BrokerGuardian(), "broker-system", conf)

  }

end Broker
