package com.sentinelpulse.broker.channels

import com.sentinelpulse.broker.channels.ChannelProtocol.{Channel, ChannelActorCommand, CleanInternalData, Save, SaveSuccess}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.duration.DurationInt

object ChannelActor:

  opaque type ChannelData = Map[Channel, Data]

  case class Data(data: Vector[Array[Byte]], insertTimestamp: Long)

  private case object TimerKey

  def apply(): Behavior[ChannelActorCommand] = {
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData(System.currentTimeMillis()), 500.millis)
      channelActor(Map.empty)
    })
  }

  def channelActor(internalData: ChannelData): Behavior[ChannelActorCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          internalData.get(channel) match {
            case Some(value) =>
              val updatedData: Vector[Array[Byte]] = value.data :+ data
              val now = System.currentTimeMillis() + ttl
              val updatedChannel = internalData + (channel -> Data(updatedData, now))
              replyTo ! SaveSuccess
              channelActor(updatedChannel)
            case None =>
              val now = System.currentTimeMillis() + ttl
              val newChannel = internalData + (channel -> Data(Vector(data), now))
              replyTo ! SaveSuccess
              channelActor(newChannel)
          }
        case CleanInternalData(pointTimeMillis) =>
          val cleanedData = internalData.filter(data => data._2.insertTimestamp >= pointTimeMillis)
          channelActor(cleanedData)
      }
    }