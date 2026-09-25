package com.sentinelpuls.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Save, SaveAck, SaveSuccess, Subscribe}
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, RegisteredChannel, SubscriberCount, UnregisteredChannel}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, ManualTime}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.duration.DurationInt

class ChannelActorTest extends AnyWordSpecLike with BeforeAndAfterAll with Matchers:
  val testKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "A channel actor" should {
    "return a SaveSuccess message when a Save message is sent" in {
      val manager = testKit.spawn(Behaviors.ignore[BrokerCommand])
      val channelActor = testKit.spawn(ChannelActor(manager))
      val probe = testKit.createTestProbe[SaveAck]()

      channelActor ! Save("test", ByteString.empty(), 1000L, probe.ref)

      probe.expectMessage(SaveSuccess)
    }

    "return a PullResponse message when an actor is subscribed and a new message is saved" in {
      val manager = testKit.spawn(Behaviors.ignore[BrokerCommand])

      val channelActor = testKit.spawn(ChannelActor(manager))
      val testDataByte = ByteString.copyFromUtf8("Test")

      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber = testKit.createTestProbe[PullResponse]()

      channelActor ! Subscribe("test", subscriber.ref, false)
      channelActor ! Save("test", testDataByte, 10000L, producer.ref)

      producer.expectMessage(SaveSuccess)
      val message = subscriber.receiveMessage()

      message.channel shouldBe "test"
      testDataByte.toString("UTF-8") shouldBe message.payload.toString("UTF-8")
    }

    "return all PullResponse messages stores in the channel when the subscriber sends the sendStoredData flag" in {
      val manager = testKit.spawn(Behaviors.ignore[BrokerCommand])

      val channelActor = testKit.spawn(ChannelActor(manager))
      val oneTestDataByte = ByteString.copyFromUtf8("Test")
      val twoTestDataByte = ByteString.copyFrom(Array(123.toByte))

      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber = testKit.createTestProbe[PullResponse]()

      channelActor ! Save("test", oneTestDataByte, 10000L, producer.ref)
      producer.expectMessage(SaveSuccess)
      channelActor ! Save("test", twoTestDataByte, 10000L, producer.ref)
      producer.expectMessage(SaveSuccess)

      channelActor ! Subscribe("test", subscriber.ref, true)

      val expectedResponse1 = PullResponse("test", oneTestDataByte)
      val expectedResponse2 = PullResponse("test", twoTestDataByte)

      subscriber.expectMessage(expectedResponse1)
      subscriber.expectMessage(expectedResponse2)
    }

    "clean the stored messages when the TTL is passed" in {

      val localTestKit = ActorTestKit(ManualTime.config)
      val manualTime = ManualTime()(localTestKit.internalSystem)

      val manager = localTestKit.spawn(Behaviors.ignore[BrokerCommand])

      val channelActor = localTestKit.spawn(ChannelActor(manager))
      val oneTestDataByte = ByteString.copyFromUtf8("Test")
      val twoTestDataByte = ByteString.copyFrom(Array(123.toByte))

      val producer = localTestKit.createTestProbe[SaveAck]()
      val subscriber = localTestKit.createTestProbe[PullResponse]()

      channelActor ! Save("test", oneTestDataByte, 1L, producer.ref)
      channelActor ! Save("test", twoTestDataByte, 1L, producer.ref)

      manualTime.timePasses(4.minutes)

      channelActor ! Subscribe("test", subscriber.ref, true)

      subscriber.expectNoMessage()
    }


    "remove the subscriber from its internal state when it dies" in {
      val manager = testKit.createTestProbe[BrokerCommand]()
      val channelActor = testKit.spawn(ChannelActor(manager.ref))

      val subscriber1 = testKit.spawn(Behaviors.ignore[PullResponse])
      val subscriber2 = testKit.createTestProbe[PullResponse]()
      val producer = testKit.spawn(Behaviors.ignore[SaveAck])


      channelActor ! Subscribe("test1", subscriber1.ref, false)
      manager.expectMessage(SubscriberCount(1, channelActor.ref))
      channelActor ! Subscribe("test2", subscriber2.ref, false)
      manager.expectMessage(SubscriberCount(2, channelActor.ref))

      testKit.stop(subscriber1.ref)

      manager.expectMessage(SubscriberCount(1, channelActor.ref))

      channelActor ! Save("test2", ByteString.copyFromUtf8("more"), 10000L, producer.ref)

      subscriber2.expectMessage(PullResponse("test2", ByteString.copyFromUtf8("more")))
    }

    "send RegisteredChannel to the manager when a Save is processed" in {
      val manager = testKit.createTestProbe[BrokerCommand]()
      val channelActor = testKit.spawn(ChannelActor(manager.ref))
      val producer = testKit.createTestProbe[SaveAck]()

      channelActor ! Save("test", ByteString.copyFromUtf8("data"), 10000L, producer.ref)

      producer.expectMessage(SaveSuccess)
      manager.expectMessage(RegisteredChannel("test", channelActor.ref))
    }

    "broadcast a message to all subscribers of the same channel" in {
      val manager = testKit.spawn(Behaviors.ignore[BrokerCommand])
      val channelActor = testKit.spawn(ChannelActor(manager))
      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber1 = testKit.createTestProbe[PullResponse]()
      val subscriber2 = testKit.createTestProbe[PullResponse]()

      channelActor ! Subscribe("test", subscriber1.ref, false)
      channelActor ! Subscribe("test", subscriber2.ref, false)
      channelActor ! Save("test", ByteString.copyFromUtf8("data"), 10000L, producer.ref)

      producer.expectMessage(SaveSuccess)
      subscriber1.expectMessageType[PullResponse]
      subscriber2.expectMessageType[PullResponse]
    }

    "not notify subscribers of a different channel" in {
      val manager = testKit.spawn(Behaviors.ignore[BrokerCommand])
      val channelActor = testKit.spawn(ChannelActor(manager))
      val producer = testKit.createTestProbe[SaveAck]()
      val subscriberA = testKit.createTestProbe[PullResponse]()
      val subscriberB = testKit.createTestProbe[PullResponse]()

      channelActor ! Subscribe("A", subscriberA.ref, false)
      channelActor ! Subscribe("B", subscriberB.ref, false)
      channelActor ! Save("A", ByteString.copyFromUtf8("a-data"), 10000L, producer.ref)

      producer.expectMessage(SaveSuccess)
      subscriberA.expectMessageType[PullResponse]
      subscriberB.expectNoMessage()
    }

    "keep a channel alive when it has subscribers but no messages" in {
      val localTestKit = ActorTestKit(ManualTime.config)
      val manualTime = ManualTime()(localTestKit.internalSystem)
      val manager = localTestKit.createTestProbe[BrokerCommand]()
      val channelActor = localTestKit.spawn(ChannelActor(manager.ref))
      val subscriber = localTestKit.createTestProbe[PullResponse]()
      val producer = localTestKit.createTestProbe[SaveAck]()

      channelActor ! Save("test", ByteString.copyFromUtf8("data"), 1L, producer.ref)
      channelActor ! Subscribe("test", subscriber.ref, false)

      producer.expectMessage(SaveSuccess)
      manager.expectMessage(RegisteredChannel("test", channelActor.ref))
      manager.expectMessage(SubscriberCount(1, channelActor.ref))

      manualTime.timePasses(4.minutes)

      manager.expectNoMessage()
    }

    "unregister a channel when it has no messages and no subscribers" in {
      val localTestKit = ActorTestKit(ManualTime.config)
      val manualTime = ManualTime()(localTestKit.internalSystem)
      val manager = localTestKit.createTestProbe[BrokerCommand]()
      val channelActor = localTestKit.spawn(ChannelActor(manager.ref))
      val producer = localTestKit.createTestProbe[SaveAck]()

      channelActor ! Save("test", ByteString.copyFromUtf8("data"), 1L, producer.ref)
      producer.expectMessage(SaveSuccess)
      manager.expectMessage(RegisteredChannel("test", channelActor.ref))

      manualTime.timePasses(4.minutes)

      manager.expectMessage(UnregisteredChannel("test", channelActor.ref))
    }

    "clean only expired messages while keeping newer ones" in {
      val localTestKit = ActorTestKit(ManualTime.config)
      val manualTime = ManualTime()(localTestKit.internalSystem)
      val manager = localTestKit.spawn(Behaviors.ignore[BrokerCommand])
      val channelActor = localTestKit.spawn(ChannelActor(manager))
      val producer = localTestKit.createTestProbe[SaveAck]()
      val subscriber = localTestKit.createTestProbe[PullResponse]()

      channelActor ! Save("test", ByteString.copyFromUtf8("old"), 1L, producer.ref)
      channelActor ! Save("test", ByteString.copyFromUtf8("old2"), 1L, producer.ref)

      manualTime.timePasses(2.minutes)

      channelActor ! Save("test", ByteString.copyFromUtf8("new"), 10000L, producer.ref)
      producer.expectMessage(SaveSuccess)

      channelActor ! Subscribe("test", subscriber.ref, true)

      val response = subscriber.expectMessageType[PullResponse]
      response.payload shouldBe ByteString.copyFromUtf8("new")
      subscriber.expectNoMessage()
    }
  }




