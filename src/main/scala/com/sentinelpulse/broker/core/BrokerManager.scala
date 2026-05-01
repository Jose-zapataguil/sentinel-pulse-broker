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
          context.log.info(s"Received a request to $channel")
          channelActors.find(_.channelMetadata.contains(channel)) match {
            case Some(metadata) =>
              client ! metadata.actor
              Behaviors.same

            case None =>
              val (lessLoadedActor, restChannelActors) = getLessLoadedActorAndRest(channelActors)
              client ! lessLoadedActor.actor

              val updatedChannelActors = lessLoadedActor.copy(
                channelMetadata = lessLoadedActor.channelMetadata + (channel -> Metadata(ttl))
              )
              brokerManager(updatedChannelActors :: restChannelActors)
          }

        case AddSubscriber(channelName, subscriber, sendStoredData) =>
          context.log.info("Received new subscriber for channel " + channelName)
          channelActors.find(_.channelMetadata.contains(channelName)) match {
            case Some(value) =>
              value.actor ! Subscribe(channelName, subscriber, sendStoredData)
              val updatedChannelActors = channelActors.map {
                case a if a.actor == value.actor =>
                  a.copy(numberOfSubscribers = a.numberOfSubscribers + 1)
                case a => a
              }
              brokerManager(updatedChannelActors)
            case None =>
              val (lessLoadedActor, restChannelActors) = getLessLoadedActorAndRest(channelActors)
              lessLoadedActor.actor ! Subscribe(channelName, subscriber, sendStoredData)

              val updatedActor = lessLoadedActor.copy(
                channelMetadata = lessLoadedActor.channelMetadata + (channelName -> Metadata(DEFAULT_TTL_TIME)),
                numberOfSubscribers = lessLoadedActor.numberOfSubscribers + 1
              )
              brokerManager(updatedActor :: restChannelActors)
          }
        case SubscriberCount(count, targetActor) =>
          val updatedChannelActors = channelActors.map {
            case a if a.actor == targetActor => a.copy(numberOfSubscribers = count)
            case a => a
          }
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
  

