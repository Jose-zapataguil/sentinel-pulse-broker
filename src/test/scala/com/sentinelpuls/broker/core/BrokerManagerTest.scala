package com.sentinelpuls.broker.core

import com.sentinelpulse.broker.channels.ChannelProtocol.ChannelActorCommand
import com.sentinelpulse.broker.core.BrokerManager
import com.sentinelpulse.broker.core.BrokerManager.{AddSubscriber, GetOrSetActorForChannel, RegisteredChannel, UnregisteredChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BrokerManagerTest extends AnyWordSpecLike with Matchers with BeforeAndAfterAll:
  
  val testKit = ActorTestKit()

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "A broker manager for strict order mode" should {
    val strictOrder = "strict-order"
    "return an ActorRef when a new producer send data to a channel" in {
      val manager = testKit.spawn(BrokerManager(1, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)

      probe.expectMessageType[ActorRef[ChannelActorCommand]]
    }

    "return the same Actor when a new producer send data to an existing channel" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)
      val actor1 = probe.receiveMessage()

      manager ! GetOrSetActorForChannel("test", 2L, probe.ref)
      val actor2 = probe.receiveMessage()

      actor1 shouldBe actor2
    }

    "return the actor with less load" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
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

    "persist the channel assignment when subscribing before the first message" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("test", 1L, probe.ref)
      probe.receiveMessage()

      val subscriber = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("new-channel", subscriber.ref)

      subscriber.expectNoMessage()

      manager ! GetOrSetActorForChannel("new-channel", 1L, probe.ref)
      val actor1 = probe.receiveMessage()

      manager ! GetOrSetActorForChannel("new-channel", 2L, probe.ref)
      val actor2 = probe.receiveMessage()

      actor1 shouldBe actor2
    }

    "route new channels to the actor with fewer subscribers" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("loaded", 1L, probe.ref)
      val loadedActor = probe.receiveMessage()

      val subscriber1 = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("loaded", subscriber1.ref)

      Thread.sleep(100)

      manager ! GetOrSetActorForChannel("light", 1L, probe.ref)
      val lightActor = probe.receiveMessage()

      val subscriber2 = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("loaded", subscriber2.ref)

      Thread.sleep(200)

      manager ! GetOrSetActorForChannel("new", 1L, probe.ref)
      val chosenActor = probe.receiveMessage()

      chosenActor shouldBe lightActor
    }

    "update channelIndex on RegisteredChannel" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()
      val externalActor = testKit.createTestProbe[ChannelActorCommand]()

      manager ! RegisteredChannel("external", externalActor.ref)

      manager ! GetOrSetActorForChannel("external", 1L, probe.ref)
      val actor = probe.receiveMessage()

      actor shouldBe externalActor.ref
    }

    "ignore UnregisteredChannel from a non-owner actor" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()
      val owner = testKit.createTestProbe[ChannelActorCommand]()
      val impostor = testKit.createTestProbe[ChannelActorCommand]()

      manager ! RegisteredChannel("guarded", owner.ref)
      manager ! UnregisteredChannel("guarded", impostor.ref)

      manager ! GetOrSetActorForChannel("guarded", 1L, probe.ref)
      val actor = probe.receiveMessage()

      actor shouldBe owner.ref
    }

    "remove channel from channelIndex when UnregisteredChannel comes from the owner" in {
      val manager = testKit.spawn(BrokerManager(2, strictOrder))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()
      val owner = testKit.createTestProbe[ChannelActorCommand]()

      manager ! RegisteredChannel("survivor", owner.ref)

      manager ! GetOrSetActorForChannel("survivor", 1L, probe.ref)
      val actorBefore = probe.receiveMessage()
      actorBefore shouldBe owner.ref

      manager ! UnregisteredChannel("survivor", owner.ref)

      manager ! GetOrSetActorForChannel("survivor", 1L, probe.ref)
      val actorAfter = probe.receiveMessage()

      actorAfter should not be owner.ref
    }

  }