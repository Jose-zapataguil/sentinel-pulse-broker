package com.sentinelpulse.broker.api

import com.sentinelpulse.broker.channels.ChannelProtocol
import com.sentinelpulse.broker.channels.ChannelProtocol.{ChannelActorCommand, Save, SaveAck}
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, GetOrSetActorForChannel}
import com.sentinelpulse.broker.proto.{ProducerService, PublishRequest, PublishSummary}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.stream.typed.scaladsl.ActorFlow
import com.google.protobuf.ByteString
import com.sentinelpulse.broker.config.ProducerParameters
import org.apache.pekko.util.Timeout
import scala.concurrent.{ExecutionContext, Future}

class ProducerServiceImpl(manager: ActorRef[BrokerCommand], parameters: ProducerParameters)
                         (using system: ActorSystem[Nothing]) extends ProducerService:
  
  given Timeout = parameters.timeout
  
  given ExecutionContext = system.executionContext

  given Scheduler = system.scheduler

  private lazy val cores = Runtime.getRuntime.availableProcessors()

  private val streamParallelism = parameters.parallelism match {
    case Some(value) => value
    case None => cores * 2
  }

  override def push(in: Source[PublishRequest, NotUsed]): Future[PublishSummary] = {
    in.prefixAndTail(1).runWith(Sink.head)
      .flatMap {
        case (head, tail) =>
          head.headOption.flatMap(_.payload.metadata) match {
            case Some(value) =>
              val futureChannel: Future[ActorRef[ChannelProtocol.ChannelActorCommand]] = {
                manager.ask(ref => GetOrSetActorForChannel(value.channel, value.ttl, ref))
              }
              futureChannel.flatMap { channelActor =>
                tail
                  .map(request => request.payload.data)
                  .collect {
                    case Some(bytes) => bytes
                  }
                  .via(ActorFlow.ask[ByteString, ChannelActorCommand, SaveAck](streamParallelism)(channelActor) {
                    (bytes, ref) => Save(value.channel, bytes, value.ttl, ref)
                  })
                  .runFold(0) { (count, result) =>
                    count + 1
                  }.map(total => PublishSummary(success = true, count = total))
              }
            case None =>
              Future.failed(new IllegalArgumentException("The first message should contains the metadata (channel and ttl)"))
          }
      }
  }
    


