package com.sentinelpulse.broker.core

import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Channel, ChannelActorCommand, Subscribe}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

object BrokerManager:

  sealed trait BrokerCommand

  case class GetOrSetActorForChannel(channelName: Channel, ttl: Long, replyTo: ActorRef[ActorRef[ChannelActorCommand]]) extends BrokerCommand

  case class AddSubscriber(channelName: Channel, subscriber: ActorRef[PullResponse], sendStoredData: Boolean = false) extends BrokerCommand

  case class SubscriberCount(count: Int, actor: ActorRef[ChannelActorCommand]) extends BrokerCommand

  case class ActorForChannels(actor: ActorRef[ChannelActorCommand], channelMetadata: Map[Channel, Metadata], numberOfSubscribers: Int)

  case class Metadata(ttl: Long)

  private val DEFAULT_TTL_TIME = 1000L

  def apply(numberOfActors: Int): Behavior[BrokerCommand] = Behaviors.setup { context =>
    val channelActors = 0 until numberOfActors map { n =>
      val actor = context.spawn(ChannelActor(context.self), s"actor$n")
      ActorForChannels(actor, Map.empty, 0)
    }
    brokerManager(channelActors.toList)
  }

  def brokerManager(channelActors: List[ActorForChannels]): Behavior[BrokerCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case GetOrSetActorForChannel(channel, ttl, client) =>
          val actorMetadata = channelActors.find(_.channelMetadata.contains(channel))
          actorMetadata match {
            case Some(metadata) =>
              client ! metadata.actor
              brokerManager(channelActors)
            case None =>
              val (lessLoadedActor: ActorForChannels, restChannelActors: List[ActorForChannels]) =
                getLessLoadedActorAndRest(channelActors)
              client ! lessLoadedActor.actor
              val newChannel = lessLoadedActor.channelMetadata + (channel -> Metadata(DEFAULT_TTL_TIME))
              val updatedChannelActors = ActorForChannels(lessLoadedActor.actor, newChannel, 0) :: restChannelActors
              brokerManager(updatedChannelActors)
          }

        case AddSubscriber(channelName, subscriber, sendStoredData) =>
          val actorMetadata = channelActors.find(_.channelMetadata.contains(channelName))
          actorMetadata match {
            case Some(value) =>
              value.actor ! Subscribe(channelName, subscriber, sendStoredData)
              val restChannelActors = channelActors.filter(_.actor != value.actor)
              val metadata = value.channelMetadata(channelName)
              val updatedMetadata = value.channelMetadata + (channelName -> Metadata(metadata.ttl))
              val updatedChannelActors = ActorForChannels(value.actor, updatedMetadata, value.numberOfSubscribers + 1) :: restChannelActors
              brokerManager(updatedChannelActors)
            case None =>
              val (lessLoadedActor: ActorForChannels, restChannelActors: List[ActorForChannels]) = getLessLoadedActorAndRest(channelActors)
              val updatedMetadata = lessLoadedActor.channelMetadata + (channelName -> Metadata(DEFAULT_TTL_TIME))
              val updatedChannelActors = ActorForChannels(lessLoadedActor.actor, updatedMetadata, 1) :: restChannelActors
              lessLoadedActor.actor ! Subscribe(channelName, subscriber, sendStoredData)
              brokerManager(updatedChannelActors)
          }
        case SubscriberCount(count, actor) =>
          val actorForChannel = channelActors.find(_.actor == actor).get
          val restChannelActors = channelActors.filterNot(_.actor == actor)
          val updatedChannelActors = ActorForChannels(actorForChannel.actor, actorForChannel.channelMetadata, count) :: restChannelActors
          brokerManager(updatedChannelActors)
      }
    }

  private[core] def getLessLoadedActorAndRest(channelActors: List[ActorForChannels]): (ActorForChannels, List[ActorForChannels]) =
    val lessLoadedActor = channelActors.map { actorForChannels =>
      val nOfChannelsByActor = actorForChannels.channelMetadata.size
      val nOfTotalSubscribers = actorForChannels.numberOfSubscribers
      (actorForChannels, nOfTotalSubscribers + nOfChannelsByActor)
    }.minBy(_._2)._1
    val restOfActors = channelActors.filter(_.actor != lessLoadedActor.actor)
    (lessLoadedActor, restOfActors)
  

