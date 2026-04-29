package com.sentinelpuls.broker

import com.google.protobuf.ByteString
import com.sentinelpulse.broker.core.{BrokerManager, BrokerServer}
import com.sentinelpulse.broker.proto.PublishRequest.Payload
import com.sentinelpulse.broker.proto.PublishRequest.Payload.Data
import com.sentinelpulse.broker.proto.{ConsumerServiceClient, ProducerServiceClient, PublishMetadata, PublishRequest, PullRequest, PullResponse}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
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

    val manager = spawn(BrokerManager(4), "test-integration-manager")

    given ActorSystem[Nothing] = system

    val grpcServer = new BrokerServer(manager, ip, port)
    serverBinding = Await.result(grpcServer.run(), 5.seconds)

    val clientSettings = GrpcClientSettings
      .connectToServiceAt("127.0.0.1", 8080)
      .withTls(false)

    producerClient = ProducerServiceClient(clientSettings)
    consumerClient = ConsumerServiceClient(clientSettings)

  override def afterAll(): Unit =
    producerClient.close()
    consumerClient.close()
    serverBinding.unbind()
    super.afterAll()


  "A grpc broker" should {
    "allow a client to consume messages published by another client" in {

      val testChannel = "test-channel"
      val testPayload = ByteString.copyFrom("Test message", "UTF-8")

      val subRequest = PullRequest(testChannel)

      val consumerStream = consumerClient.pull(subRequest)

      val streamProbe = consumerStream.runWith(TestSink[PullResponse]())

      streamProbe.request(1)

      val publishMetadata = PublishRequest(
        Payload.Metadata(
          PublishMetadata(testChannel, 1000L)
        ))

      val metadataSource = Source.single(publishMetadata)

      val publishData = PublishRequest(Data(testPayload))
      
      val dataSource = Source.tick(initialDelay = 0.millis, interval = 100.millis, "tick")
        .map(_ => publishData).take(10)


      val producerStream = metadataSource.concat(dataSource)

      producerClient.push(producerStream)

      val receivedMessage = streamProbe.expectNext(5.seconds)

      receivedMessage.payload shouldBe testPayload

      streamProbe.cancel()
    }
  }


end BrokerIntegrationTest

