package com.sentinelpuls.broker

import com.sentinelpulse.broker.Broker
import com.sentinelpulse.broker.config.{BrokerParameters, OverflowPolicy, ProducerParameters}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class BrokerConfigTest extends AnyWordSpecLike with Matchers:

  private def baseConfig: String =
    """
      |broker {
      |  ip = "127.0.0.1"
      |  port = 8080
      |  actors = 4
      |  mode = "strict-order"
      |  max-messages-per-channel = 10000
      |  overflowPolicy = "drop-oldest"
      |  producer {
      |    timeout = 2 seconds
      |  }
      |}
      |""".stripMargin

  private def configWith(overrides: String): com.typesafe.config.Config =
    ConfigFactory.parseString(overrides)
      .withFallback(ConfigFactory.parseString(baseConfig))

  "Broker.loadParameters" should {
    "parse a valid config with all fields" in {
      val conf = configWith(
        """
        broker {
          producer {
            parallelism = 16
          }
        }
      """)

      val result = Broker.loadParameters(conf)

      result shouldBe a[Right[_, _]]
      val params = result.getOrElse(fail("Expected Right but got Left"))
      params.ip shouldBe "127.0.0.1"
      params.port shouldBe 8080
      params.nOfActors shouldBe 4
      params.storageMode shouldBe "strict-order"
      params.maxMsgPerChannel shouldBe 10000
      params.producerParameters.timeout shouldBe 2.seconds
      params.producerParameters.parallelism shouldBe Some(16)
      params.overflowPolicy shouldBe OverflowPolicy.DropOldest
    }

    "parse a valid config without optional parallelism" in {
      val conf = configWith("")

      val result = Broker.loadParameters(conf)

      result shouldBe a[Right[_, _]]
      val params = result.getOrElse(fail("Expected Right but got Left"))
      params.producerParameters.parallelism shouldBe None
    }

    "parse backpressure with default timeout" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "backpressure"
        }
      """)

      val result = Broker.loadParameters(conf)

      result shouldBe a[Right[_, _]]
      val params = result.getOrElse(fail("Expected Right but got Left"))
      params.overflowPolicy shouldBe OverflowPolicy.Backpressure(2.seconds)
    }

    "parse backpressure with custom timeout" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "backpressure"
          backpressure-timeout = 5 seconds
        }
      """)

      val result = Broker.loadParameters(conf)

      result shouldBe a[Right[_, _]]
      val params = result.getOrElse(fail("Expected Right but got Left"))
      params.overflowPolicy shouldBe OverflowPolicy.Backpressure(5.seconds)
    }

    "fail on unknown overflow policy" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "unknown-policy"
        }
      """)

      val result = Broker.loadParameters(conf)

      result shouldBe a[Left[_, _]]
      result.left.getOrElse(fail("Expected Left but got Right")) should include("Unknow overflow policy")
    }
  }

  "BrokerParameters.validate" should {
    val producerParams = ProducerParameters(2.seconds, Some(8))

    "accept any policy in strict-order mode" in {
      val policies = List(
        OverflowPolicy.DropOldest,
        OverflowPolicy.Reject,
        OverflowPolicy.Backpressure(2.seconds)
      )

      policies.foreach { policy =>
        val params = BrokerParameters(
          "127.0.0.1", 8080, 4, "strict-order", 10000, producerParams, policy
        )
        val result = BrokerParameters.validate(params)
        result shouldBe a[Right[_, _]]
      }
    }

    "accept drop-oldest in high-throughput mode" in {
      val params = BrokerParameters(
        "127.0.0.1", 8080, 4, "high-throughput", 10000, producerParams, OverflowPolicy.DropOldest
      )
      BrokerParameters.validate(params) shouldBe a[Right[_, _]]
    }

    "accept reject in high-throughput mode" in {
      val params = BrokerParameters(
        "127.0.0.1", 8080, 4, "high-throughput", 10000, producerParams, OverflowPolicy.Reject
      )
      BrokerParameters.validate(params) shouldBe a[Right[_, _]]
    }

    "reject backpressure in high-throughput mode" in {
      val params = BrokerParameters(
        "127.0.0.1", 8080, 4, "high-throughput", 10000, producerParams, OverflowPolicy.Backpressure(2.seconds)
      )
      val result = BrokerParameters.validate(params)
      result shouldBe a[Left[_, _]]
      result.left.getOrElse(fail("Expected Left but got Right")) should include("Backpressure is not supported")
    }

    "reject unknown storage mode" in {
      val params = BrokerParameters(
        "127.0.0.1", 8080, 4, "unknown-mode", 10000, producerParams, OverflowPolicy.DropOldest
      )
      val result = BrokerParameters.validate(params)
      result shouldBe a[Left[_, _]]
      result.left.getOrElse(fail("Expected Left but got Right")) should include("Unknown storage mode")
    }
  }

  "Broker.getOverflowPolicy" should {
    "return DropOldest for drop-oldest" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "drop-oldest"
        }
      """)
      Broker.getOverflowPolicy(conf) shouldBe Right(OverflowPolicy.DropOldest)
    }

    "return Reject for reject" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "reject"
        }
      """)
      Broker.getOverflowPolicy(conf) shouldBe Right(OverflowPolicy.Reject)
    }

    "return Backpressure with default timeout for backpressure" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "backpressure"
        }
      """)
      Broker.getOverflowPolicy(conf) shouldBe Right(OverflowPolicy.Backpressure(2.seconds))
    }

    "return Backpressure with custom timeout" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "backpressure"
          backpressure-timeout = 10 seconds
        }
      """)
      Broker.getOverflowPolicy(conf) shouldBe Right(OverflowPolicy.Backpressure(10.seconds))
    }

    "fail on unknown policy" in {
      val conf = configWith(
        """
        broker {
          overflowPolicy = "unknown"
        }
      """)
      val result = Broker.getOverflowPolicy(conf)
      result shouldBe a[Left[_, _]]
      result.left.getOrElse(fail("Expected Left but got Right")) should include("Unknow overflow policy")
    }
  }
