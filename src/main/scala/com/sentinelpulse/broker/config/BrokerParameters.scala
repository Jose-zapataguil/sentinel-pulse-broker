package com.sentinelpulse.broker.config

import scala.concurrent.duration.FiniteDuration


case class BrokerParameters(ip: String,
                            port: Int,
                            nOfActors: Int,
                            storageMode: String,
                            producerParameters: ProducerParameters
                           )


case class ProducerParameters(timeout: FiniteDuration, parallelism: Option[Int])