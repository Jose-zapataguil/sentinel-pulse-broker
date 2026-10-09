package com.sentinelpuls.broker

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.config.OverflowPolicy.DropOldest
import com.sentinelpulse.broker.config.{BrokerParameters, ProducerParameters}
import com.sentinelpulse.broker.core.{BrokerManager, BrokerServer}
import com.sentinelpulse.broker.proto.PublishRequest.Payload
import com.sentinelpulse.broker.proto.PublishRequest.Payload.Data
import com.sentinelpulse.broker.proto.{ConsumerServiceClient, ProducerServiceClient, PublishMetadata, PublishRequest, PullRequest, PullResponse}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.*
import scala.concurrent.Await

class BrokerHighThroughputIntegrationTest extends ScalaTestWithActorTestKit(testConfig) with AnyWordSpecLike with BeforeAndAfterAll:

  var serverBinding: ServerBinding = _
  var producerClient: ProducerServiceClient = _
  var consumerClient: ConsumerServiceClient = _

  val ip = "127.0.0.1"
  val port = 8081

  override def beforeAll(): Unit =
    super.beforeAll()

    val manager = spawn(BrokerManager(4, "high-throughput"), "test-ht-integration-manager")

    given ActorSystem[Nothing] = system

    val parameters = BrokerParameters(ip, port, 4, "high-throughput", 100 ,ProducerParameters(2.seconds, Some(8)), DropOldest)

    val grpcServer = new BrokerServer(manager, parameters)
    serverBinding = Await.result(grpcServer.run(), 5.seconds)

    val clientSettings = GrpcClientSettings
      .connectToServiceAt("127.0.0.1", port)
      .withTls(false)

    producerClient = ProducerServiceClient(clientSettings)
    consumerClient = ConsumerServiceClient(clientSettings)

  override def afterAll(): Unit =
    Await.result(producerClient.close(), 5.seconds)
    Await.result(consumerClient.close(), 5.seconds)
    Await.result(serverBinding.unbind(), 5.seconds)
    Await.result(Http()(system).shutdownAllConnectionPools(), 5.seconds)
    super.afterAll()


  "A grpc broker in high-throughput mode" should {
    "allow a client to consume messages published by another client" in {

      val testChannel = "ht-test-channel"
      val testPayload = ByteString.copyFrom("HT Test message", "UTF-8")

      val publishMetadata = PublishRequest(
        Payload.Metadata(
          PublishMetadata(testChannel, 1000L)
        ))
      val dataMessages = (1 to 10).map(_ => PublishRequest(Data(testPayload)))
      val producerStream = Source(publishMetadata :: dataMessages.toList)

      Await.result(producerClient.push(producerStream), 5.seconds)

      val subRequest = PullRequest(testChannel, allMessages = true)
      val consumerStream = consumerClient.pull(subRequest)
      val streamProbe = consumerStream.runWith(TestSink[PullResponse]())
      streamProbe.request(1)

      val receivedMessage = streamProbe.expectNext(5.seconds)
      receivedMessage.payload shouldBe testPayload
      streamProbe.cancel()
    }

    "deliver multiple messages from round-robin producers to a single consumer" in {
      val testChannel = "ht-multi-channel"

      val publishMetadata = PublishRequest(
        Payload.Metadata(
          PublishMetadata(testChannel, 1000L)
        ))
      val dataMessages = (1 to 10).map { i =>
        PublishRequest(Data(ByteString.copyFromUtf8(s"Message-$i")))
      }
      val producerStream = Source(publishMetadata :: dataMessages.toList)

      Await.result(producerClient.push(producerStream), 5.seconds)

      val subRequest = PullRequest(testChannel, allMessages = true)
      val consumerStream = consumerClient.pull(subRequest)
      val streamProbe = consumerStream.runWith(TestSink[PullResponse]())
      streamProbe.request(10)

      val messages = streamProbe.within(5.seconds) {
        streamProbe.expectNextN(10)
      }
      messages should have size 10
      streamProbe.cancel()
    }

    "broadcast messages to multiple consumers" in {
      val testChannel = "ht-broadcast-channel"

      val publishMetadata = PublishRequest(
        Payload.Metadata(
          PublishMetadata(testChannel, 1000L)
        ))
      val dataMessages = (1 to 3).map { i =>
        PublishRequest(Data(ByteString.copyFromUtf8(s"Broadcast-$i")))
      }
      val producerStream = Source(publishMetadata :: dataMessages.toList)

      Await.result(producerClient.push(producerStream), 5.seconds)

      val subRequest1 = PullRequest(testChannel, allMessages = true)
      val subRequest2 = PullRequest(testChannel, allMessages = true)

      val streamProbe1 = consumerClient.pull(subRequest1).runWith(TestSink[PullResponse]())
      val streamProbe2 = consumerClient.pull(subRequest2).runWith(TestSink[PullResponse]())

      streamProbe1.request(3)
      streamProbe2.request(3)

      val messages1 = streamProbe1.within(5.seconds) {
        streamProbe1.expectNextN(3)
      }
      val messages2 = streamProbe2.within(5.seconds) {
        streamProbe2.expectNextN(3)
      }

      messages1 should have size 3
      messages2 should have size 3

      streamProbe1.cancel()
      streamProbe2.cancel()
    }
  }

end BrokerHighThroughputIntegrationTest
