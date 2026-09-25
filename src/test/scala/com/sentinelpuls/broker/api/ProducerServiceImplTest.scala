package com.sentinelpuls.broker.api

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.api.ProducerServiceImpl
import com.sentinelpulse.broker.config.ProducerParameters
import com.sentinelpulse.broker.core.BrokerManager
import com.sentinelpulse.broker.proto.{PublishMetadata, PublishRequest}
import com.sentinelpulse.broker.proto.PublishRequest.Payload.{Data, Metadata}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt
import scala.concurrent.Await

class ProducerServiceImplTest extends AnyWordSpecLike with Matchers with BeforeAndAfterAll:

  val testKit = ActorTestKit()

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "A producer" should {
    "return a PublishSummary object when the stream ends in strict-order mode" in {

      given ActorSystem[Nothing] = testKit.system

      val manager = testKit.spawn(BrokerManager(2, "strict-order"))

      val producerService = new ProducerServiceImpl(manager, ProducerParameters(2.seconds, Some(8)))

      val payloadTest1 = ByteString.copyFromUtf8("TEST")
      val payloadTest2 = ByteString.copyFromUtf8("test")

      val source = Source(
        List(
          PublishRequest(
            Metadata(PublishMetadata("test1", 1000L))
          ),
          PublishRequest(Data(payloadTest1)),
          PublishRequest(Data(payloadTest2)),

        )
      )
      val future = producerService.push(source)
      val summary = Await.result(future, 1.second)

      summary.count shouldBe 2
    }

    "return a PublishSummary object when the stream ends in high-throughput mode" in {

      given ActorSystem[Nothing] = testKit.system

      val manager = testKit.spawn(BrokerManager(2, "high-throughput"))

      val producerService = new ProducerServiceImpl(manager, ProducerParameters(2.seconds, Some(8)))

      val source = Source(
        List(
          PublishRequest(
            Metadata(PublishMetadata("test1", 1000L))
          ),
          PublishRequest(Data(ByteString.copyFromUtf8("TEST"))),
          PublishRequest(Data(ByteString.copyFromUtf8("test"))),

        )
      )
      val future = producerService.push(source)
      val summary = Await.result(future, 1.second)

      summary.count shouldBe 2
    }

    "fail when the first message does not contain metadata" in {

      given ActorSystem[Nothing] = testKit.system

      val manager = testKit.spawn(BrokerManager(2, "strict-order"))

      val producerService = new ProducerServiceImpl(manager, ProducerParameters(2.seconds, Some(8)))

      val source = Source.single(PublishRequest(Data(ByteString.copyFromUtf8("no-metadata"))))

      val future = producerService.push(source)

      val exception = intercept[IllegalArgumentException] {
        Await.result(future, 1.second)
      }
      exception.getMessage should include("metadata")
    }
  }

end ProducerServiceImplTest