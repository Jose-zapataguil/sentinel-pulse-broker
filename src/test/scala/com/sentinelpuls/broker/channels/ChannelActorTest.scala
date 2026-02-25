package com.sentinelpuls.broker.channels

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.channels.ChannelActor
import com.sentinelpulse.broker.channels.ChannelProtocol.{Save, SaveAck, SaveSuccess, Subscribe}
import com.sentinelpulse.broker.proto.PullResponse
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, ManualTime}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class ChannelActorTest extends AnyWordSpecLike with BeforeAndAfterAll with Matchers:
  val testKit = ActorTestKit(ManualTime.config)

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "A channel actor" should {
    "return a SaveSuccess message when a Save message is sent" in {
      val channelActor = testKit.spawn(ChannelActor())
      val probe = testKit.createTestProbe[SaveAck]()

      channelActor ! Save("test", Array(), 1000L, probe.ref)

      probe.expectMessage(SaveSuccess)
    }

    "return a PullResponse message when an actor is subscribed and a new message is saved" in {
      val channelActor = testKit.spawn(ChannelActor())
      val testDataByte = "Test".getBytes

      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber = testKit.createTestProbe[PullResponse]()

      channelActor ! Subscribe("test", subscriber.ref, false)
      channelActor ! Save("test", testDataByte, 10000L, producer.ref)

      producer.expectMessage(SaveSuccess)
      val message = subscriber.receiveMessage()

      message.channel shouldBe "test"
      testDataByte should contain theSameElementsAs message.payload.toByteArray
    }

    "return all PullResponse messages stores in the channel when the subscriber sends the sendStoredData flag" in {
      val channelActor = testKit.spawn(ChannelActor())
      val oneTestDataByte = "Test".getBytes
      val twoTestDataByte = Array(123.toByte)

      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber = testKit.createTestProbe[PullResponse]()

      channelActor ! Save("test", oneTestDataByte, 10000L, producer.ref)
      channelActor ! Save("test", twoTestDataByte, 10000L, producer.ref)

      channelActor ! Subscribe("test", subscriber.ref, true)

      producer.expectMessage(SaveSuccess)

      val expectedResponse1 = PullResponse("test", ByteString.copyFrom(oneTestDataByte))
      val expectedResponse2 = PullResponse("test", ByteString.copyFrom(twoTestDataByte))

      subscriber.expectMessage(expectedResponse1)
      subscriber.expectMessage(expectedResponse2)
    }

    "clean the stored messages when the TTL is passed" in {
      val channelActor = testKit.spawn(ChannelActor())
      val oneTestDataByte = "Test".getBytes
      val twoTestDataByte = Array(123.toByte)

      val producer = testKit.createTestProbe[SaveAck]()
      val subscriber = testKit.createTestProbe[PullResponse]()

      channelActor ! Save("test", oneTestDataByte, 1L, producer.ref)
      channelActor ! Save("test", twoTestDataByte, 1L, producer.ref)

      val manualTime = ManualTime()(testKit.system)

      manualTime.timePasses(50.millis)


      channelActor ! Subscribe("test", subscriber.ref, true)

      subscriber.expectNoMessage()
    }
  }




