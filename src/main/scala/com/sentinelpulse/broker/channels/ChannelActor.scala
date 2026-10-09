package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.*
import com.sentinelpulse.broker.config.OverflowPolicy
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, RegisteredChannel, SubscriberCount, UnregisteredChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt

object ChannelActor:

  private enum SaveDecision:
    case Accept(queue: Queue[DataEnvelope])
    case Reject
    case Defer
  
  opaque type Channels = Map[Channel, ChannelData]

  opaque type Subscribers = Map[Channel, Set[ActorRef[PullResponse]]]

  private case class ChannelData(messages: Queue[DataEnvelope], ttl: Long)

  private case class DataEnvelope(payload: ByteString, timestamp: Long)

  private case class Config(managerRef: ActorRef[BrokerCommand], queueMaxSize: Int, overflowPolicy: OverflowPolicy)

  private case class State(channels: Channels, subscribers: Subscribers)

  private case object TimerKey

  def apply(managerRef: ActorRef[BrokerCommand], queueMaxSize: Int = Int.MaxValue, overflowPolicy: OverflowPolicy = OverflowPolicy.DropOldest): Behavior[ChannelActorCommand] =
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData, 2.minutes)
      channelActor(Config(managerRef, queueMaxSize, overflowPolicy), State(Map.empty, Map.empty))
    })

  private def channelActor(config: Config, state: State): Behavior[ChannelActorCommand] = {
    Behaviors.receive[ChannelActorCommand] { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          val now = System.currentTimeMillis()
          val currentChannel = state.channels.getOrElse(channel, ChannelData(Queue.empty, ttl))
          val expirationTimeMillis = now - currentChannel.ttl
          val cleanedQueue = currentChannel.messages.dropWhile(_.timestamp < expirationTimeMillis)

          resolveSave(cleanedQueue, DataEnvelope(data, now), config.queueMaxSize, config.overflowPolicy) match {
            case SaveDecision.Accept(updatedQueue) =>
              val pullResponse = PullResponse(channel, data)

              state.subscribers.get(channel).foreach(subscribers => subscribers.foreach(_ ! pullResponse))

              replyTo ! SaveSuccess
              config.managerRef ! RegisteredChannel(channel, context.self)
              channelActor(
                config,
                state.copy(channels = state.channels.updated(channel, currentChannel.copy(messages = updatedQueue)))
              )
    
            case SaveDecision.Reject =>
              replyTo ! SaveFailure
              channelActor(
                config,
                state.copy(channels = state.channels.updated(channel, currentChannel.copy(messages = cleanedQueue)))
              )
            case SaveDecision.Defer => ???
          }
          

        case CleanInternalData =>
          val now = System.currentTimeMillis()

          val cleanedData = state.channels.flatMap { channel =>
            val expirationTimeMillis = now - channel._2.ttl
            val updatedChannelData = channel._2.messages.dropWhile(_.timestamp < expirationTimeMillis)
            if updatedChannelData.nonEmpty || state.subscribers.contains(channel._1) then
              Some(channel._1 -> channel._2.copy(messages = updatedChannelData))
            else {
              config.managerRef ! UnregisteredChannel(channel._1, context.self)
              None
            }
          }
          channelActor(config, state.copy(channels = cleanedData))

        case Subscribe(channel, actor, sendStoredData) =>
          context.watch(actor)
          val updatedSubscribers = state.subscribers.updatedWith(channel) {
            case Some(refs) => Some(refs + actor)
            case None => Some(Set(actor))
          }
          if sendStoredData then {
            state.channels.get(channel) match {
              case Some(value) =>
                context.log.debug(s"Sending ${value.messages.size} messages stored")
                value.messages.foreach(message =>
                  actor ! PullResponse(channel, message.payload)
                )
              case None =>
                context.log.debug(s"No data stored for channel '$channel'")
            }
          }
          config.managerRef ! SubscriberCount(updatedSubscribers.values.map(_.size).sum, context.self)
          channelActor(config, state.copy(subscribers = updatedSubscribers))
      }
    }.receiveSignal {
      case (context, Terminated(deadActor)) =>
        val typedDeadActor = deadActor.unsafeUpcast[PullResponse]
        val updatedSubscribers = state.subscribers.map {
          case (channel, refs) => channel -> (refs - typedDeadActor)
        }.filter(_._2.nonEmpty)

        config.managerRef ! SubscriberCount(updatedSubscribers.values.map(_.size).sum, context.self)
        channelActor(config, state.copy(subscribers = updatedSubscribers))
    }
  }

  private def resolveSave(queue: Queue[DataEnvelope],
                          message: DataEnvelope,
                          maxSize: Int,
                          policy: OverflowPolicy): SaveDecision = policy match {
    case OverflowPolicy.DropOldest =>
      val enqueued = queue.enqueue(message)
      val updatedQueue = if enqueued.size > maxSize then enqueued.drop(enqueued.size - maxSize) else enqueued
      SaveDecision.Accept(updatedQueue)
    case OverflowPolicy.Reject =>
      if queue.size >= maxSize then SaveDecision.Reject
      else SaveDecision.Accept(queue.enqueue(message))
    case OverflowPolicy.Backpressure(timeout) => ???
  }

