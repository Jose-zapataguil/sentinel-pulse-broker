package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.*
import com.sentinelpulse.broker.core.BrokerManager.SubscriberCount
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.concurrent.duration.DurationInt

object ChannelActor:

  opaque type ChannelData = Map[Channel, Data]

  opaque type Subscribers = Map[ActorRef[PullResponse], Channel]

  private case class Data(data: Vector[Array[Byte]], insertTimestamp: Long)

  private case object TimerKey

  def apply(): Behavior[ChannelActorCommand] =
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData(System.currentTimeMillis()), 50.millis)
      channelActor(Map.empty, Map.empty)
    })

  def channelActor(internalData: ChannelData, subscribers: Subscribers): Behavior[ChannelActorCommand] =
    Behaviors.receive[ChannelActorCommand] { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          val pullResponse = PullResponse(channel, ByteString.copyFrom(data))
          sendToSubscribers(subscribers.filter(_._2 == channel).keys.toList, pullResponse)
          val newInternalData = internalData.get(channel) match {
            case Some(value) =>
              val updatedData: Vector[Array[Byte]] = value.data :+ data
              val now = System.currentTimeMillis() + ttl
              val updatedChannel = internalData + (channel -> Data(updatedData, now))
              replyTo ! SaveSuccess
              updatedChannel
            case None =>
              val now = System.currentTimeMillis() + ttl
              val newChannel = internalData + (channel -> Data(Vector(data), now))
              replyTo ! SaveSuccess
              newChannel
          }
          channelActor(newInternalData, subscribers)

        case CleanInternalData(pointTimeMillis) =>
          val cleanedData = internalData.filter(data => data._2.insertTimestamp <= pointTimeMillis)
          channelActor(cleanedData, subscribers)
        case Subscribe(channel, actor, sendStoredData) =>
          context.watch(actor)
          val updatedSubscribers = subscribers + (actor -> channel)
          if sendStoredData then
            internalData.get(channel) match {
              case Some(value) =>
                value.data.map(d => PullResponse(channel, ByteString.copyFrom(d)))
                  .foreach(message => actor ! message)
              case None =>
            }
          channelActor(internalData, updatedSubscribers)
        case GetSubscriberCount(replyTo) =>
          replyTo ! SubscriberCount(subscribers.size, context.self)
          channelActor(internalData, subscribers)
      }
    }.receiveSignal {
      case (context, Terminated(deadActor)) =>
        val typedDeadActor = deadActor.asInstanceOf[ActorRef[PullResponse]]
        val updatedSubscribers = subscribers - typedDeadActor
        channelActor(internalData, updatedSubscribers)
    }

  private def sendToSubscribers(subscribers: List[ActorRef[PullResponse]], message: PullResponse): Unit =
    subscribers.foreach(_ ! message)
