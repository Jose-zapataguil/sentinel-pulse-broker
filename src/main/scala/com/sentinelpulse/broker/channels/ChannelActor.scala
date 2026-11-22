package com.sentinelpulse.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.*
import com.sentinelpulse.broker.config.OverflowPolicy
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, RegisteredChannel, SubscriberCount, UnregisteredChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import java.time.Clock
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

object ChannelActor:

  private enum SaveDecision:
    case Accept(queue: Queue[DataEnvelope])
    case Reject
    case Defer

  opaque type Channels = Map[Channel, ChannelData]

  opaque type Subscribers = Map[Channel, Set[ActorRef[PullResponse]]]

  opaque type PendingState = Map[Channel, Queue[PendingMessage]]

  private case class ChannelData(messages: Queue[DataEnvelope], ttl: Long)

  private case class DataEnvelope(payload: ByteString, timestamp: Long)

  private case class Config(
                             managerRef: ActorRef[BrokerCommand],
                             queueMaxSize: Int,
                             maxPendingMsgSize: Int,
                             overflowPolicy: OverflowPolicy,
                             clock: Clock
                           )

  private case class State(channels: Channels, subscribers: Subscribers, pending: PendingState)

  private case class PendingMessage(payload: ByteString,
                                    ttl: Long,
                                    replyTo: ActorRef[SaveAck],
                                    timestamp: Long,
                                    deadline: Long
                                   )

  private case object TimerKey

  def apply(managerRef: ActorRef[BrokerCommand],
            queueMaxSize: Int = Int.MaxValue,
            overflowPolicy: OverflowPolicy = OverflowPolicy.DropOldest,
            maxPendingMsg: Int = Int.MaxValue,
            clock: Clock = Clock.systemUTC()
           ): Behavior[ChannelActorCommand] =
    Behaviors.withTimers(timers => {
      timers.startTimerWithFixedDelay(TimerKey, CleanInternalData, 2.minutes)
      channelActor(Config(managerRef, queueMaxSize, maxPendingMsg, overflowPolicy, clock), State(Map.empty, Map.empty, Map.empty))
    })

  private def channelActor(config: Config, state: State): Behavior[ChannelActorCommand] = {
    Behaviors.receive[ChannelActorCommand] { (context, message) =>
      message match {
        case Save(channel, data, ttl, replyTo) =>
          val now = config.clock.millis()
          val currentChannel = state.channels.getOrElse(channel, ChannelData(Queue.empty, ttl))
          val expirationTimeMillis = now - currentChannel.ttl
          val cleanedQueue = currentChannel.messages.dropWhile(_.timestamp < expirationTimeMillis)
          val pendingQueue = state.pending.getOrElse(channel, Queue.empty)

          resolveSave(cleanedQueue, DataEnvelope(data, now), config.queueMaxSize, config.overflowPolicy) match {
            case SaveDecision.Accept(updatedQueue) =>
              broadcast(state, channel, data)
              replyTo ! SaveSuccess
              config.managerRef ! RegisteredChannel(channel, context.self)
              val newState = state.copy(channels = state.channels.updated(channel, currentChannel.copy(messages = updatedQueue)))
              channelActor(
                config,
                newState,
              )

            case SaveDecision.Reject =>
              replyTo ! SaveFailure
              channelActor(
                config,
                state.copy(channels = state.channels.updated(channel, currentChannel.copy(messages = cleanedQueue))),
              )
            case SaveDecision.Defer =>
              val bsTimeout = backpressureTimeout(config.overflowPolicy)
              context.scheduleOnce(bsTimeout, context.self, CleanInternalData)
              channelActor(config, defer(config, channel, PendingMessage(data, ttl, replyTo, now, now + bsTimeout.toMillis), state))
          }


        case CleanInternalData =>
          val now = config.clock.millis()

          val cleanedData = state.copy(channels = cleanChannels(state, config, context, now))
          val finalState = cleanedData.pending.foldLeft(cleanedData)((st, p) => free(config, p._1, st, now))
          channelActor(config, finalState)

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

  private def backpressureTimeout(policy: OverflowPolicy): FiniteDuration = policy match {
    case OverflowPolicy.Backpressure(timeout) => timeout
    case _ => Duration.Zero
  }

  private def cleanChannels(state: State, config: Config, context: ActorContext[ChannelActorCommand], now: Long): Channels =
    state.channels.flatMap { channel =>
      val expirationTimeMillis = now - channel._2.ttl
      val updatedChannelData = channel._2.messages.dropWhile(_.timestamp < expirationTimeMillis)
      if updatedChannelData.nonEmpty || state.subscribers.contains(channel._1) then
        Some(channel._1 -> channel._2.copy(messages = updatedChannelData))
      else {
        config.managerRef ! UnregisteredChannel(channel._1, context.self)
        rejectDeferred(channel._1, state.pending)
        None
      }
    }

  private def rejectDeferred(channel: Channel, pending: PendingState): Unit =
    pending.get(channel).foreach(_.foreach(_.replyTo ! SaveFailure))

  private def broadcast(state: State, channel: Channel, data: ByteString): Unit =
    val pullResponse = PullResponse(channel, data)
    state.subscribers.get(channel).foreach(subscribers => subscribers.foreach(_ ! pullResponse))


  private def defer(config: Config, channel: Channel, pm: PendingMessage, state: State): State =
    val pendingQueue = state.pending.getOrElse(channel, Queue.empty)
    if pendingQueue.size >= config.maxPendingMsgSize then
      pm.replyTo ! SaveFailure
      state
    else
      state.copy(pending = state.pending.updated(channel, pendingQueue.enqueue(pm)))

  private def free(config: Config, channel: Channel, state: State, now: Long): State =

    @tailrec
    def freeWhile(state: State, pendingQ: Queue[PendingMessage], continue: Boolean): State =
      val cd = state.channels(channel)
      val cleaned = cd.messages.dropWhile(_.timestamp < now - cd.ttl)
      pendingQ match {
        case h +: t if continue =>
          if h.deadline <= now then
            h.replyTo ! SaveFailure
            freeWhile(state, t, true)
          else
            resolveSave(cleaned, DataEnvelope(h.payload, h.timestamp), config.queueMaxSize, config.overflowPolicy) match {
              case SaveDecision.Accept(updated) =>
                val newState = state.copy(
                  channels = state.channels.updated(channel, cd.copy(messages = updated)),
                )
                broadcast(newState, channel, h.payload)
                h.replyTo ! SaveSuccess
                freeWhile(newState, t, true)

              case _ => freeWhile(state, pendingQ, false)
            }
        case _ => endFree(pendingQ, state)
      }

    def endFree(pendingQ: Queue[PendingMessage], state: State): State =
      if pendingQ.isEmpty then state.copy(pending = state.pending - channel)
      else state.copy(pending = state.pending.updated(channel, pendingQ))

    freeWhile(state, state.pending.getOrElse(channel, Queue.empty), true)


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
    case OverflowPolicy.Backpressure(timeout) =>
      if queue.size >= maxSize then SaveDecision.Defer
      else SaveDecision.Accept(queue.enqueue(message))
  }

