package com.sentinelpulse.broker.core

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}

object BrokerGuardian:

  def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
    context.log.info("Starting broker")

    val manager = context.spawn(BrokerManager(8), "broker-manager")

    given ActorSystem[Nothing] = context.system

    val grpcServer = new BrokerServer(manager)
    grpcServer.run()

    Behaviors.empty
  }

end BrokerGuardian
