package com.sentinelpuls.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Save, SaveAck, SaveSuccess, Subscribe}
import com.sentinelpulse.broker.core.BrokerManager.{BrokerCommand, SubscriberCount}
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
      channelActor ! Save("test", twoTestDataByte, 10000L, producer.ref)

      Thread.sleep(100)

      channelActor ! Subscribe("test", subscriber.ref, true)

      producer.expectMessage(SaveSuccess)

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
      channelActor ! Subscribe("test2", subscriber2.ref, false)

      Thread.sleep(100)
      manager.expectMessage(SubscriberCount(1, channelActor.ref))
      manager.expectMessage(SubscriberCount(2, channelActor.ref))

      testKit.stop(subscriber1.ref)

      Thread.sleep(100)
      
      manager.expectMessage(SubscriberCount(1, channelActor.ref))

      channelActor ! Save("test2", ByteString.copyFromUtf8("more"), 10000L, producer.ref)

      subscriber2.expectMessage(PullResponse("test2", ByteString.copyFromUtf8("more")))
    }
  }




