package com.sentinelpulse.broker.api

import com.sentinelpulse.broker.core.BrokerManager.{AddSubscriber, BrokerCommand}
import com.sentinelpulse.broker.proto.{ConsumerService, PullRequest, PullResponse}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.stream.{Materializer, OverflowStrategy}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.typed.scaladsl.ActorSource

class ConsumerServiceImpl(manager: ActorRef[BrokerCommand])(using ActorSystem[Nothing]) extends ConsumerService:

  override def pull(in: PullRequest): Source[PullResponse, NotUsed] =

    val (subscriberActor: ActorRef[PullResponse], stream: Source[PullResponse, NotUsed]) = ActorSource.actorRef[PullResponse](
      completionMatcher = PartialFunction.empty,
      failureMatcher = PartialFunction.empty,
      bufferSize = 1000,
      overflowStrategy = OverflowStrategy.dropHead
    ).preMaterialize()

    manager ! AddSubscriber(in.channel, subscriberActor, in.allMessages)
    
    stream


