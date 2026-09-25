package com.sentinelpulse.broker.core

import com.sentinelpulse.broker.api.{ConsumerServiceImpl, ProducerServiceImpl}
import com.sentinelpulse.broker.config.BrokerParameters
import com.sentinelpulse.broker.core.BrokerManager.BrokerCommand
import com.sentinelpulse.broker.proto.{ConsumerService, ConsumerServiceHandler, ProducerService, ProducerServiceHandler}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.grpc.scaladsl.{ServerReflection, ServiceHandler}
import org.apache.pekko.http.scaladsl.Http
import scala.concurrent.Future

class BrokerServer(manager: ActorRef[BrokerCommand], brokerParameters: BrokerParameters)(using system: ActorSystem[Nothing]):

  def run(): Future[Http.ServerBinding] =

    val producerService = ProducerServiceHandler.partial(
      new ProducerServiceImpl(manager, brokerParameters.producerParameters)
    )
    val consumerService = ConsumerServiceHandler.partial(new ConsumerServiceImpl(manager))
    val reflectionService = ServerReflection.partial(List(ProducerService, ConsumerService))

    val serviceHandlers = ServiceHandler.concatOrNotFound(producerService, consumerService, reflectionService)

    Http()
      .newServerAt(brokerParameters.ip, brokerParameters.port)
      .bind(serviceHandlers)

end BrokerServer
