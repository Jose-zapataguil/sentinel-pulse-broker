package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
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


