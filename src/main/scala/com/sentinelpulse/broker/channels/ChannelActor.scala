package com.sentinelpulse.broker.channels

import com.sentinelpulse.broker.channels.ChannelProtocol.{Channel, ChannelActorCommand, Save, SaveSuccess}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object ChannelActor:

  opaque type ChannelData = Map[Channel, Data]

  case class Data(data: Vector[Array[Byte]], ttl: Long)

  def apply(): Behavior[ChannelActorCommand] = channelActor(Map.empty)
  
  def channelActor(internalData: ChannelData): Behavior[ChannelActorCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          internalData.get(channel) match {
            case Some(value) =>
              val updatedData: Vector[Array[Byte]] = value.data :+ data
              val updatedChannel = internalData + (channel -> Data(updatedData, value.ttl))
              replyTo ! SaveSuccess
              channelActor(updatedChannel)
            case None =>
              val newChannel = internalData + (channel -> Data(Vector(data), ttl))
              replyTo ! SaveSuccess
              channelActor(newChannel)
          }
      }
    }