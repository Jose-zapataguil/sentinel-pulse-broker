package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.ActorRef


object ChannelProtocol:

  type Channel = String

  sealed trait SaveAck

  case object SaveSuccess extends SaveAck

  case object SaveFailure extends SaveAck

  sealed trait ChannelActorCommand

  case class Save(channel: String,
                  payload: ByteString,
                  ttl: Long,
                  replyTo: ActorRef[SaveAck]
                 ) extends ChannelActorCommand

  case object CleanInternalData extends ChannelActorCommand
  
  case class Subscribe(channel: Channel, actor: ActorRef[PullResponse], sendStoredData: Boolean) 
    extends ChannelActorCommand


