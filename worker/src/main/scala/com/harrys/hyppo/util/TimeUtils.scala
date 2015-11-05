package com.harrys.hyppo.util

import java.time._
import java.util.{Date, TimeZone}

import scala.concurrent.duration.FiniteDuration

/**
 * Created by jpetty on 11/3/15.
 */
private[hyppo] object TimeUtils {
  //  Shared instances of UTC TimeZone objects
  final val UTCZoneOffset = ZoneOffset.UTC
  final val UTCTimeZoneId = ZoneId.of(UTCZoneOffset.getId)
  final val UTCTimeZone   = TimeZone.getTimeZone(UTCTimeZoneId)

  def currentLocalDateTime(): LocalDateTime = {
    LocalDateTime.now(UTCTimeZoneId)
  }

  def currentLegacyDate() : Date = {
    javaLegacyDate(currentLocalDateTime())
  }

  def toLocalDateTime(legacyDate: Date) : LocalDateTime = {
    ZonedDateTime.ofInstant(legacyDate.toInstant, UTCTimeZoneId).toLocalDateTime
  }

  def javaLegacyDate(dateTime: LocalDateTime) : Date = {
    Date.from(dateTime.atZone(UTCTimeZoneId).toInstant)
  }

  def javaDuration(duration: FiniteDuration) : Duration = {
    Duration.ofNanos(duration.toNanos)
  }

  def scalaDuration(duration: Duration) : FiniteDuration = {
    FiniteDuration(duration.toNanos, scala.concurrent.duration.NANOSECONDS)
  }
}
