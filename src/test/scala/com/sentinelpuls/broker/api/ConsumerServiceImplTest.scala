package com.sentinelpuls.broker.api

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.api.ConsumerServiceImpl
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import com.sentinelpulse.broker.core.BrokerManager.{AddSubscriber, BrokerCommand}
import com.sentinelpulse.broker.proto.{PullRequest, PullResponse}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.testkit.scaladsl.TestSink

class ConsumerServiceImplTest extends AnyWordSpecLike with Matchers:

  val testKit = ActorTestKit()

  "A consumer service" should {
    "generate a Source with the incoming messages" in {

      given ActorSystem[Nothing] = testKit.system

      val mockManager = testKit.createTestProbe[BrokerCommand]()

      val consumerService = new ConsumerServiceImpl(mockManager.ref)

      val pullRequest = PullRequest("test", true)

      val stream = consumerService.pull(pullRequest)

      val streamProbe = stream.runWith(TestSink[PullResponse]())

      val receivedMsg = mockManager.expectMessageType[AddSubscriber]

      receivedMsg.channelName shouldBe "test"
      receivedMsg.sendStoredData shouldBe true

      val subscriberActor = receivedMsg.subscriber

      val testMsg1 = PullResponse("test", ByteString.copyFrom("hello".getBytes))
      val testMsg2 = PullResponse("test", ByteString.copyFrom("world".getBytes))

      subscriberActor ! testMsg1
      subscriberActor ! testMsg2

      streamProbe.request(2)
        .expectNext(testMsg1)
        .expectNext(testMsg2)


      streamProbe.cancel()
    }
  }

end ConsumerServiceImplTest

