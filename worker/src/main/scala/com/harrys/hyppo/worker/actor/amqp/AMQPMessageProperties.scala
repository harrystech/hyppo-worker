package com.harrys.hyppo.worker.actor.amqp

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.{Date, UUID}

import com.rabbitmq.client.AMQP.BasicProperties

/**
 * Created by jpetty on 10/29/15.
 */
object AMQPMessageProperties {
  final val SerializationMimeType = "application/x-java-serialized-object"
  private final val UTCZoneId = ZoneId.of("UTC")
  private final val ExpirationFormat = DateTimeFormatter.ISO_ZONED_DATE_TIME

  final case class HyppoQueueProperties(correlationId: UUID, replyQueue: String, startedAt: LocalDateTime, expiredAt: LocalDateTime) {
    def isExpired: Boolean = {
      expiredAt.isBefore(LocalDateTime.now(UTCZoneId))
    }
    def workAge: Duration = Duration.between(startedAt, LocalDateTime.now(UTCZoneId))
  }

  def enqueueProperties(correlationId: UUID, resultQueue: String, startedAt: LocalDateTime, expire: LocalDateTime) : BasicProperties = {
    enqueueProperties(HyppoQueueProperties(correlationId, resultQueue, startedAt, expire))
  }

  def enqueueProperties(headers: HyppoQueueProperties) : BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .expiration(headers.expiredAt.atZone(UTCZoneId).format(ExpirationFormat))
      .timestamp(Date.from(headers.startedAt.atZone(UTCZoneId).toInstant))
      .replyTo(headers.replyQueue)
      .correlationId(headers.correlationId.toString)
      .build()
  }

  def parseItemProperties(properties: BasicProperties) : HyppoQueueProperties = {
    val replyQueue  = properties.getReplyTo
    val correlation = UUID.fromString(properties.getCorrelationId)
    val expiration  = ZonedDateTime.parse(properties.getExpiration, ExpirationFormat).toLocalDateTime
    val startedAt   = ZonedDateTime.ofInstant(properties.getTimestamp.toInstant, UTCZoneId).toLocalDateTime
    HyppoQueueProperties(correlation, replyQueue, startedAt, expiration)
  }
}
