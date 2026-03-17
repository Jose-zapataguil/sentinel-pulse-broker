package com.sentinelpulse.broker.channels

import com.sentinelpulse.broker.core.BrokerManager.SubscriberCount
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.ActorRef


object ChannelProtocol:

  type Channel = String

  sealed trait SaveAck

  case object SaveSuccess extends SaveAck

  case object SaveFailure extends SaveAck

  sealed trait ChannelActorCommand

  case class Save(channel: String,
                  payload: Array[Byte],
                  ttl: Long,
                  replyTo: ActorRef[SaveAck]
                 ) extends ChannelActorCommand

  case class CleanInternalData(pointTimeMillis: Long) extends ChannelActorCommand
  
  case class Subscribe(channel: Channel, actor: ActorRef[PullResponse], sendStoredData: Boolean) 
    extends ChannelActorCommand

  case class GetSubscriberCount(replyTo: ActorRef[SubscriberCount]) extends ChannelActorCommand


