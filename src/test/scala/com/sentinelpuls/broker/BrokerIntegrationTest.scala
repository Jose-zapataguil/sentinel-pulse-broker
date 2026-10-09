package com.sentinelpuls.broker

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.config.OverflowPolicy.DropOldest
import com.sentinelpulse.broker.config.{BrokerParameters, ProducerParameters}
import com.sentinelpulse.broker.core.{BrokerManager, BrokerServer}
import com.sentinelpulse.broker.proto.PublishRequest.Payload
import com.sentinelpulse.broker.proto.PublishRequest.Payload.Data
import com.sentinelpulse.broker.proto.{ConsumerServiceClient, ProducerServiceClient, PublishMetadata, PublishRequest, PullRequest, PullResponse}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.*
import scala.concurrent.Await

val testConfig = ConfigFactory.parseString("pekko.http.server.preview.enable-http2 = on")
  .withFallback(ConfigFactory.load())

class BrokerIntegrationTest extends ScalaTestWithActorTestKit(testConfig) with AnyWordSpecLike with BeforeAndAfterAll:

  var serverBinding: ServerBinding = _
  var producerClient: ProducerServiceClient = _
  var consumerClient: ConsumerServiceClient = _
  
  val ip = "127.0.0.1"
  val port = 8080

  override def beforeAll(): Unit =
    super.beforeAll()

    val manager = spawn(BrokerManager(4, "strict-order"), "test-integration-manager")

    given ActorSystem[Nothing] = system

    val parameters = BrokerParameters(ip, port, 4, "strict-order", 10000, ProducerParameters(2.seconds, Some(8)), DropOldest)

    val grpcServer = new BrokerServer(manager, parameters)
    serverBinding = Await.result(grpcServer.run(), 5.seconds)

    val clientSettings = GrpcClientSettings
      .connectToServiceAt("127.0.0.1", 8080)
      .withTls(false)

    producerClient = ProducerServiceClient(clientSettings)
    consumerClient = ConsumerServiceClient(clientSettings)

  override def afterAll(): Unit =
    Await.result(producerClient.close(), 5.seconds)
    Await.result(consumerClient.close(), 5.seconds)
    Await.result(serverBinding.unbind(), 5.seconds)
    Await.result(Http()(system).shutdownAllConnectionPools(), 5.seconds)
    super.afterAll()


  "A grpc broker" should {
    "allow a client to consume messages published by another client" in {

      val testChannel = "test-channel"
      val testPayload = ByteString.copyFrom("Test message", "UTF-8")

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
  }


end BrokerIntegrationTest

