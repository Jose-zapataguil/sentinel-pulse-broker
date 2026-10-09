package com.sentinelpulse.broker.config

import scala.concurrent.duration.{FiniteDuration, pairIntToDuration}

enum OverflowPolicy:
  case DropOldest extends OverflowPolicy
  case Reject extends OverflowPolicy
  case Backpressure(timeout: FiniteDuration) extends OverflowPolicy

case class BrokerParameters(ip: String,
                            port: Int,
                            nOfActors: Int,
                            storageMode: String,
                            maxMsgPerChannel: Int,
                            producerParameters: ProducerParameters,
                            overflowPolicy: OverflowPolicy
                           )


object BrokerParameters:

  def validate(params: BrokerParameters): Either[String, BrokerParameters] =
    params.storageMode match {
      case "strict-order" =>
        Right(params)
      case "high-throughput" =>
        params.overflowPolicy match {
          case _: OverflowPolicy.Backpressure =>
            Left("Backpressure is not supported in high-throughput mode")
          case _ => Right(params)
        }
      case other => Left(s"Unknown storage mode: $other")
    }


case class ProducerParameters(timeout: FiniteDuration, parallelism: Option[Int])
