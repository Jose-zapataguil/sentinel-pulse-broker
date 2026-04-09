package com.sentinelpulse.broker.core

import com.sentinelpulse.broker.config.BrokerParameters
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}

object BrokerGuardian:

  def apply(brokerParameters: BrokerParameters): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
    context.log.info("Starting broker")

    val manager = context.spawn(BrokerManager(brokerParameters.nOfActors), "broker-manager")

    given ActorSystem[Nothing] = context.system

    val grpcServer = new BrokerServer(manager, brokerParameters.ip, brokerParameters.port)
    grpcServer.run()

    Behaviors.empty
  }

end BrokerGuardian
