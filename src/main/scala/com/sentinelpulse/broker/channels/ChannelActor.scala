package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.*
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, SubscriberCount}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt

object ChannelActor:

  opaque type Channels = Map[Channel, ChannelData]

  opaque type Subscribers = Map[Channel, Set[ActorRef[PullResponse]]]

  private case class ChannelData(messages: Queue[DataEnvelope], ttl: Long)

  private case class DataEnvelope(payload: Array[Byte], timestamp: Long)

  private case object TimerKey

  def apply(managerRef: ActorRef[BrokerCommand]): Behavior[ChannelActorCommand] =
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData, 2.minutes)
      channelActor(managerRef, Map.empty, Map.empty)
    })

  def channelActor(managerRef: ActorRef[BrokerCommand], channels: Channels, subscribers: Subscribers): Behavior[ChannelActorCommand] =
    Behaviors.receive[ChannelActorCommand] { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          val pullResponse = PullResponse(channel, ByteString.copyFrom(data))

          subscribers.get(channel)
            .foreach(subscribers => subscribers.foreach(_ ! pullResponse))

          val now = System.currentTimeMillis()
          val currentChannel = channels.getOrElse(channel, ChannelData(Queue.empty, ttl))

          val newMessage = DataEnvelope(data, now)
          val updatedQueue = currentChannel.messages.enqueue(newMessage)

          val expirationTimeMillis = now - currentChannel.ttl
          val cleanedQueue = updatedQueue.dropWhile(_.timestamp < expirationTimeMillis)

          val updatedChannels = channels.updated(channel, currentChannel.copy(messages = cleanedQueue))

          replyTo ! SaveSuccess

          channelActor(managerRef, updatedChannels, subscribers)

        case CleanInternalData =>
          val now = System.currentTimeMillis()

          val cleanedData = channels.flatMap { channel =>
              val expirationTimeMillis = now - channel._2.ttl
              val updatedChannelData = channel._2.messages.dropWhile(_.timestamp < expirationTimeMillis)
              if updatedChannelData.nonEmpty then
                Some(channel._1 -> channel._2.copy(messages = updatedChannelData))
              else
                None
          }
          channelActor(managerRef, cleanedData, subscribers)
        case Subscribe(channel, actor, sendStoredData) =>
          context.watch(actor)
          val updatedSubscribers = subscribers.updatedWith(channel) {
            case Some(refs) => Some(refs + actor)
            case None => Some(Set(actor))
          }
          if sendStoredData then {
            channels.get(channel) match {
              case Some(value) =>
                context.log.info(s"Sending ${value.messages.size} messages stored")
                value.messages.foreach(message =>
                  actor ! PullResponse(channel, ByteString.copyFrom(message.payload))
                )
              case None =>
                context.log.info(s"No data stored for channel '$channel'")
            }
          }
          managerRef ! SubscriberCount(updatedSubscribers.values.map(_.size).sum, context.self)
          channelActor(managerRef, channels, updatedSubscribers)
      }
    }.receiveSignal {
      case (context, Terminated(deadActor)) =>
        val typedDeadActor = deadActor.unsafeUpcast[PullResponse]
        val removedSubForChannel = subscribers.find(_._2.contains(typedDeadActor))
          .map(v => v._1 -> v._2.filter(_ == deadActor)).get
        val updatedSubscribers = subscribers + (removedSubForChannel._1 -> removedSubForChannel._2)

        managerRef ! SubscriberCount(updatedSubscribers.size, context.self)
        channelActor(managerRef, channels, updatedSubscribers)
    }
