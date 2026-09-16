package com.sentinelpulse.broker.core

import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Channel, ChannelActorCommand, Subscribe}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, Routers}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

object BrokerManager:

  sealed trait BrokerCommand

  case class GetOrSetActorForChannel(channelName: Channel, ttl: Long, replyTo: ActorRef[ActorRef[ChannelActorCommand]]) extends BrokerCommand

  case class AddSubscriber(channelName: Channel, subscriber: ActorRef[PullResponse], sendStoredData: Boolean = false) extends BrokerCommand

  case class RegisteredChannel(channel: Channel, actor: ActorRef[ChannelActorCommand]) extends BrokerCommand

  case class UnregisteredChannel(channel: Channel, actor: ActorRef[ChannelActorCommand]) extends BrokerCommand

  case class SubscriberCount(count: Int, actor: ActorRef[ChannelActorCommand]) extends BrokerCommand

  case class State(
                    channelIndex: Map[Channel, ActorRef[ChannelActorCommand]],
                    actorStats: Map[ActorRef[ChannelActorCommand], Int],
                  )

  def apply(numberOfActors: Int, storageMode: String): Behavior[BrokerCommand] = Behaviors.setup { context =>
    if (storageMode == "high-throughput") {
      context.log.info(s"Starting broker in $storageMode mode")
      val pool = Routers.pool(numberOfActors)(ChannelActor(context.self))
        .withBroadcastPredicate {
          case _: Subscribe => true
          case _ => false
        }
      val routerRef = context.spawn(pool, "router-pool")
      highThroughputBehavior(routerRef, Map.empty)
    } else {
      val channelActors = 0 until numberOfActors map { n =>
        context.spawn(ChannelActor(context.self), s"actor$n")
      }
      val initialStats = channelActors.map(_ -> 0).toMap
      strictOrderBehavior(State(Map.empty, initialStats))
    }
  }

  def highThroughputBehavior(
                              router: ActorRef[ChannelActorCommand],
                              channelIndex: Map[Channel, Set[ActorRef[ChannelActorCommand]]]
                            ): Behavior[BrokerCommand] = Behaviors.receiveMessage {
    case GetOrSetActorForChannel(_, _, replyTo) =>
      replyTo ! router
      Behaviors.same

    case AddSubscriber(channelName, subscriber, sendStoredData) =>
      router ! Subscribe(channelName, subscriber, sendStoredData)
      Behaviors.same
    case SubscriberCount(_, _) => Behaviors.same
    case RegisteredChannel(channel, actor) =>
      val updatedSet = channelIndex.getOrElse(channel, Set.empty) + actor
      highThroughputBehavior(router, channelIndex.updated(channel, updatedSet))
    case UnregisteredChannel(channel, actor) =>
      val updatedSet = channelIndex.getOrElse(channel, Set.empty) - actor
      if (updatedSet.isEmpty) {
        highThroughputBehavior(router, channelIndex - channel)
      } else {
        highThroughputBehavior(router, channelIndex.updated(channel, updatedSet))
      }
  }

  def strictOrderBehavior(state: State): Behavior[BrokerCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case GetOrSetActorForChannel(channel, ttl, client) =>
          context.log.info(s"Received a request to $channel")
          state.channelIndex.get(channel) match {
            case Some(actor) =>
              client ! actor
              Behaviors.same

            case None =>
              val lessLoadedActor = getLessLoadedActor(state.actorStats)
              client ! lessLoadedActor

              strictOrderBehavior(state.copy(channelIndex = state.channelIndex + (channel -> lessLoadedActor)))
          }

        case AddSubscriber(channelName, subscriber, sendStoredData) =>
          context.log.info("Received new subscriber for channel " + channelName)
          state.channelIndex.get(channelName) match {
            case Some(actor) =>
              actor ! Subscribe(channelName, subscriber, sendStoredData)
              strictOrderBehavior(state)

            case None =>
              val lessLoadedActor = getLessLoadedActor(state.actorStats)
              lessLoadedActor ! Subscribe(channelName, subscriber, sendStoredData)

              strictOrderBehavior(state.copy(channelIndex = state.channelIndex + (channelName -> lessLoadedActor)))
          }

        case SubscriberCount(count, targetActor) =>
          val newState = state.copy(
            actorStats = state.actorStats.updated(targetActor, count)
          )
          strictOrderBehavior(newState)

        case RegisteredChannel(channel, actor) =>
          val newState = state.copy(
            channelIndex = state.channelIndex + (channel -> actor)
          )
          strictOrderBehavior(newState)

        case UnregisteredChannel(channel, actor) =>
          val newState =
            if state.channelIndex.get(channel).contains(actor) then
              state.copy(channelIndex = state.channelIndex - channel)
            else state
          strictOrderBehavior(newState)
      }
    }

  // Uncompleted
  private[core] def getLessLoadedActor(actorStats: Map[ActorRef[ChannelActorCommand], Int]): ActorRef[ChannelActorCommand] = actorStats.minBy(_._2)._1

  

