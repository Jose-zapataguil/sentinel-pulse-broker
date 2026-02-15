package com.sentinelpulse.broker.api

import com.sentinelpulse.broker.channels.ChannelProtocol
import com.sentinelpulse.broker.channels.ChannelProtocol.Save
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, GetOrSetActorForChannel}
import com.sentinelpulse.broker.proto.{ProducerService, PublishRequest, PublishSummary}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

class ProducerServiceImpl(manager: ActorRef[BrokerCommand])(using system: ActorSystem[_]) extends ProducerService:

  given timeout: Timeout = 2.seconds

  given ExecutionContext = system.executionContext

  given Scheduler = system.scheduler

  override def push(in: Source[PublishRequest, NotUsed]): Future[PublishSummary] = {
    in.map { request =>
      val futureChannel: Future[ActorRef[ChannelProtocol.ChannelActorCommand]] =
        manager.ask(ref => GetOrSetActorForChannel(request.channel, 0L, ref))

      val data = request.payload.toByteArray

      futureChannel.flatMap(channelActor =>
        channelActor.ask(ref => Save(request.channel, data, 0L, ref))
      )
    }.runFold(0) { (count, result) =>
      count + 1
    }.map(total => PublishSummary(success = true, count = total))
    
  }
    


