package com.sentinelpuls.broker.api

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.api.ProducerServiceImpl
import com.sentinelpulse.broker.core.BrokerManager
import com.sentinelpulse.broker.proto.PublishRequest
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt
import scala.concurrent.Await

class ProducerServiceImplTest extends AnyWordSpecLike with Matchers:

  val testKit = ActorTestKit()

  "A producer" should {
    "return a PublishSummary object when the stream ends" in {

      given ActorSystem[Nothing] = testKit.system

      val manager = testKit.spawn(BrokerManager(2))

      val producerService = new ProducerServiceImpl(manager)

      val payloadTest = ByteString.copyFromUtf8("TEST")
      val source = Source(
        List(
          PublishRequest("test", 1000L, payloadTest),
          PublishRequest("test2", 10000L, payloadTest)
        )
      )
      val future = producerService.push(source)
      val summary = Await.result(future, 1.second)

      summary.count shouldBe 2
    }
  }

end ProducerServiceImplTest