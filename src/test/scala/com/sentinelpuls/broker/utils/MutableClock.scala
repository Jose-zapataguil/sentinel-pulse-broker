package com.sentinelpuls.broker.utils

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import scala.concurrent.duration.FiniteDuration

final class MutableClock(private var current: Instant) extends Clock:
  override def getZone: ZoneId = ZoneOffset.UTC

  override def instant(): Instant = current

  override def withZone(zone: ZoneId): Clock = this

  def advanceBy(d: FiniteDuration): Unit = current = current.plusMillis(d.toMillis)

