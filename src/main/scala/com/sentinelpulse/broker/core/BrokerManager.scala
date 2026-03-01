package com.sentinelpulse.broker.core

import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Channel, ChannelActorCommand, Subscribe}
import com.sentinelpulse.broker.core.BrokerManager.ActorForChannel
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

object BrokerManager:

  sealed trait BrokerCommand

  case class GetOrSetActorForChannel(channelName: Channel, ttl: Long, replyTo: ActorRef[ActorRef[ChannelActorCommand]]) extends BrokerCommand

  case class AddSubscriber(channelName: Channel, subscriber: ActorRef[PullResponse], sendStoredData: Boolean = false) extends BrokerCommand

  case class ActorForChannel(actor: ActorRef[ChannelActorCommand], channelMetadata: Map[Channel, Option[Long]])


  def apply(numberOfActors: Int): Behavior[BrokerCommand] = Behaviors.setup { context =>
    val channelActors = 0 to numberOfActors map { n =>
      val actor = context.spawn(ChannelActor(), s"actor$n")
      ActorForChannel(actor, Map.empty)
    }
    brokerManager(channelActors.toList)
  }

  def brokerManager(channelActors: List[ActorForChannel]): Behavior[BrokerCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case GetOrSetActorForChannel(channel, ttl, client) =>
          val actorMetadata = channelActors.find(_.channelMetadata.contains(channel))
          actorMetadata match {
            case Some(metadata) =>
              client ! metadata.actor
              brokerManager(channelActors)
            case None =>
              val (lessLoadedActor: ActorForChannel, newChannelActors: List[ActorForChannel]) =
                getLessLoadedActorAndUpdated(channelActors, channel, Some(ttl))
              client ! lessLoadedActor.actor
              brokerManager(newChannelActors)
          }

        case AddSubscriber(channelName, subscriber, sendStoredData) =>
          val actorMetadata = channelActors.find(_.channelMetadata.contains(channelName))
          actorMetadata match {
            case Some(value) =>
              value.actor ! Subscribe(channelName, subscriber, sendStoredData)
              brokerManager(channelActors)
            case None =>
              val (lessLoadedActor: ActorForChannel, newChannelActors: List[ActorForChannel]) =
                getLessLoadedActorAndUpdated(channelActors, channelName, None)
              lessLoadedActor.actor ! Subscribe(channelName, subscriber, sendStoredData)
              brokerManager(newChannelActors)
          }
      }
    }

  private[core] def getLessLoadedActorAndUpdated(channelActors: List[ActorForChannel],
                                                 channelName: Channel,
                                                 ttl: Option[Long]): (ActorForChannel, List[ActorForChannel]) =

    val lessLoadedActor = channelActors.minBy(_.channelMetadata.size)
    val updatedMetadata = lessLoadedActor.channelMetadata + (channelName -> None)
    val newChannelActors = channelActors.tail :+ ActorForChannel(lessLoadedActor.actor, updatedMetadata)
    (lessLoadedActor, newChannelActors)
  

