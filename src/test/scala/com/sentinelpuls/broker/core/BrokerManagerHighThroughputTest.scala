package com.sentinelpuls.broker.core

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelProtocol.{ChannelActorCommand, Save, SaveAck, SaveSuccess, Subscribe}
import com.sentinelpulse.broker.core.BrokerManager
import com.sentinelpulse.broker.core.BrokerManager.{AddSubscriber, GetOrSetActorForChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class BrokerManagerHighThroughputTest extends AnyWordSpecLike with Matchers with BeforeAndAfterAll:
  val testKit = ActorTestKit()

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "A broker manager for high-throughput mode" should {
    val highThroughput = "high-throughput"

    "return the same router actor for any channel" in {
      val manager = testKit.spawn(BrokerManager(2, highThroughput))
      val probe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("channel-a", 1L, probe.ref)
      val router1 = probe.receiveMessage()

      manager ! GetOrSetActorForChannel("channel-b", 1L, probe.ref)
      val router2 = probe.receiveMessage()

      router1 shouldBe router2
    }

    "deliver messages to a subscriber that joined before the first message" in {
      val manager = testKit.spawn(BrokerManager(4, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("early-sub", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val subscriber = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("early-sub", subscriber.ref)

      val saveProbe = testKit.createTestProbe[SaveAck]()
      router ! Save("early-sub", ByteString.copyFromUtf8("hello"), 10000L, saveProbe.ref)

      saveProbe.expectMessage(5.seconds, SaveSuccess)
      val response = subscriber.expectMessageType[PullResponse](5.seconds)
      response.channel shouldBe "early-sub"
      response.payload shouldBe ByteString.copyFromUtf8("hello")
    }

    "distribute saves across the pool without loss" in {
      val manager = testKit.spawn(BrokerManager(4, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("dist", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val saveProbe = testKit.createTestProbe[SaveAck]()

      val messageCount = 20
      (1 to messageCount).foreach { i =>
        router ! Save("dist", ByteString.copyFromUtf8(s"msg-$i"), 10000L, saveProbe.ref)
      }

      val responses = saveProbe.receiveMessages(messageCount, 5.seconds)
      responses should have size messageCount
      responses.foreach(_ shouldBe SaveSuccess)
    }

    "deliver stored messages when sendStoredData is true" in {
      val manager = testKit.spawn(BrokerManager(2, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("replay", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val saveProbe = testKit.createTestProbe[SaveAck]()
      router ! Save("replay", ByteString.copyFromUtf8("first"), 10000L, saveProbe.ref)
      saveProbe.expectMessage(5.seconds, SaveSuccess)

      val lateSubscriber = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("replay", lateSubscriber.ref, sendStoredData = true)

      val response = lateSubscriber.expectMessageType[PullResponse](5.seconds)
      response.channel shouldBe "replay"
      response.payload shouldBe ByteString.copyFromUtf8("first")
    }

    "not replay stored messages when sendStoredData is false" in {
      val manager = testKit.spawn(BrokerManager(2, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("no-replay", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val saveProbe = testKit.createTestProbe[SaveAck]()
      router ! Save("no-replay", ByteString.copyFromUtf8("first"), 10000L, saveProbe.ref)
      saveProbe.expectMessage(5.seconds, SaveSuccess)

      val lateSubscriber = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("no-replay", lateSubscriber.ref)

      lateSubscriber.expectNoMessage()
    }

    "deliver multiple messages published after subscription" in {
      val manager = testKit.spawn(BrokerManager(4, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("after-sub", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val subscriber = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("after-sub", subscriber.ref)

      val saveProbe = testKit.createTestProbe[SaveAck]()
      (1 to 5).foreach { i =>
        router ! Save("after-sub", ByteString.copyFromUtf8(s"msg-$i"), 10000L, saveProbe.ref)
      }

      saveProbe.receiveMessages(5, 5.seconds)
      val responses = subscriber.receiveMessages(5, 5.seconds)
      responses.map(_.payload.toStringUtf8) should contain theSameElementsAs (1 to 5).map(i => s"msg-$i")
    }

    "handle multiple channels independently" in {
      val manager = testKit.spawn(BrokerManager(4, highThroughput))
      val routerProbe = testKit.createTestProbe[ActorRef[ChannelActorCommand]]()

      manager ! GetOrSetActorForChannel("channel-a", 1L, routerProbe.ref)
      val router = routerProbe.receiveMessage()

      val subscriberA = testKit.createTestProbe[PullResponse]()
      val subscriberB = testKit.createTestProbe[PullResponse]()
      manager ! AddSubscriber("channel-a", subscriberA.ref)
      manager ! AddSubscriber("channel-b", subscriberB.ref)

      val saveProbe = testKit.createTestProbe[SaveAck]()
      router ! Save("channel-a", ByteString.copyFromUtf8("a-only"), 10000L, saveProbe.ref)
      saveProbe.expectMessage(5.seconds, SaveSuccess)

      subscriberA.expectMessageType[PullResponse](5.seconds)
      subscriberB.expectNoMessage()
    }
  }
