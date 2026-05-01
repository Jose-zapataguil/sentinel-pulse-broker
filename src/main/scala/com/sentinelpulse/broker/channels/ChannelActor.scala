package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.*
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, SubscriberCount}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration.DurationInt

object ChannelActor:

  opaque type ChannelData = Map[Channel, Data]

  opaque type Subscribers = Map[ActorRef[PullResponse], Channel]

  private case class Data(data: Vector[Array[Byte]], limitMillis: Long)

  private case object TimerKey

  def apply(managerRef: ActorRef[BrokerCommand]): Behavior[ChannelActorCommand] =
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData, 50.millis)
      channelActor(managerRef, Map.empty, Map.empty)
    })

  def channelActor(managerRef: ActorRef[BrokerCommand], internalData: ChannelData, subscribers: Subscribers): Behavior[ChannelActorCommand] =
    Behaviors.receive[ChannelActorCommand] { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          val pullResponse = PullResponse(channel, ByteString.copyFrom(data))

          subscribers.collect { case (ref, `channel`) => ref}
            .foreach(_ ! pullResponse)

          val expirationTimeMillis = System.currentTimeMillis() + ttl

          val newInternalData = internalData.updatedWith(channel){
            case Some(value) => Some(Data(value.data :+ data, expirationTimeMillis))
            case None => Some(Data(Vector(data), expirationTimeMillis))
          }

          replyTo ! SaveSuccess
          channelActor(managerRef, newInternalData, subscribers)

        case CleanInternalData =>
          val now = System.currentTimeMillis()
          val cleanedData = internalData.filter(data => data._2.limitMillis >= now)
          channelActor(managerRef, cleanedData, subscribers)
        case Subscribe(channel, actor, sendStoredData) =>
          context.watch(actor)
          val updatedSubscribers = subscribers + (actor -> channel)
          if sendStoredData then {
            internalData.get(channel) match {
              case Some(value) =>
                context.log.info(s"Sending ${value.data.size} messages stored")
                value.data.foreach(message =>
                  actor ! PullResponse(channel, ByteString.copyFrom(message))
                )
              case None =>
                context.log.info(s"No data stored for channel '$channel'")
            }
          }
          managerRef ! SubscriberCount(updatedSubscribers.size, context.self)
          channelActor(managerRef, internalData, updatedSubscribers)
      }
    }.receiveSignal {
      case (context, Terminated(deadActor)) =>
        val typedDeadActor = deadActor.unsafeUpcast[PullResponse]
        val updatedSubscribers = subscribers - typedDeadActor
        managerRef ! SubscriberCount(updatedSubscribers.size, context.self)
        channelActor(managerRef, internalData, updatedSubscribers)
    }
