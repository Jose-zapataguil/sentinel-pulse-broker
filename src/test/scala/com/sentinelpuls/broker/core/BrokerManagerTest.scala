package com.sentinelpuls.broker.core

import com.sentinelpulse.broker.channels.ChannelProtocol.ChannelActorCommand
import com.sentinelpulse.broker.core.BrokerManager
import com.sentinelpulse.broker.core.BrokerManager.{AddSubscriber, GetOrSetActorForChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BrokerManagerTest extends AnyWordSpecLike with Matchers with BeforeAndAfterAll:
  val testKit = ActorTestKit()

  "A broker manager" should {
    "return an ActorRef when a new producer send data to a channel" in {
      val manager = testKit.spawn(BrokerManager(1))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)

      probe.expectMessageType[ActorRef[ChannelActorCommand]]
    }

    "return the same Actor when a new producer send data to an existing channel" in {
      val manager = testKit.spawn(BrokerManager(2))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)
      val actor1 = probe.receiveMessage()

      manager ! GetOrSetActorForChannel("test", 2L, probe.ref)
      val actor2 = probe.receiveMessage()

      actor1 shouldBe actor2
    }

    "return the actor with less load" in {
      val manager = testKit.spawn(BrokerManager(2))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)
      probe.expectMessageType[ActorRef[ChannelActorCommand]]

      manager ! GetOrSetActorForChannel("test1", 1L, probe.ref)
      val actor1 = probe.receiveMessage()

      val subscriber1 = testKit.createTestProbe[PullResponse]()
      val subscriber2 = testKit.createTestProbe[PullResponse]()

      manager ! AddSubscriber("test", subscriber1.ref)
      manager ! AddSubscriber("test", subscriber2.ref)

      manager ! GetOrSetActorForChannel("test2", 1L, probe.ref)

      val actor2 = probe.receiveMessage()

      actor1 shouldBe actor2
    }

    "subscribe to the actor with less load when the channel is not stored in any actor" in {
      val manager = testKit.spawn(BrokerManager(2))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)
      val actor1 = probe.receiveMessage()

      val subscriber = testKit.createTestProbe[PullResponse]()

      manager ! AddSubscriber("test1", subscriber.ref)

      subscriber.expectNoMessage()

      manager ! GetOrSetActorForChannel("test1", 1L, probe.ref)
      val actor2 = probe.receiveMessage()

      actor1 should not be actor2
    }
    
    

  }