package com.harrys.hyppo.worker.actor.amqp

import java.time.{Duration, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.{Date, UUID}

import com.rabbitmq.client.AMQP.BasicProperties

/**
 * Created by jpetty on 10/29/15.
 */
object AMQPMessageProperties {
  final val SerializationMimeType = "application/x-java-serialized-object"
  private final val UTCZoneId = ZoneId.of("UTC")

  def currentLocalDateTime: LocalDateTime = LocalDateTime.now(UTCZoneId)

  def enqueueProperties(correlationId: UUID, replyToQueue: String, expiration: scala.concurrent.duration.FiniteDuration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, currentLocalDateTime, Duration.ofMillis(expiration.toMillis))
  }


  def enqueueProperties(correlationId: UUID, replyToQueue: String, expiration: Duration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, currentLocalDateTime, expiration)
  }

  def enqueueProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, expiration: scala.concurrent.duration.FiniteDuration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, startedAt, Duration.ofMillis(expiration.toMillis))
  }

  def enqueueProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, expiration: Duration) : BasicProperties = {
    val props = HyppoMessageProperties(correlationId, replyToQueue, startedAt, expiration)
    enqueueProperties(props)
  }

  def enqueueProperties(headers: HyppoMessageProperties) : BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .expiration(headers.timeToLive.toMillis.toString)
      .timestamp(Date.from(headers.startedAt.atZone(UTCZoneId).toInstant))
      .replyTo(headers.replyToQueue)
      .correlationId(headers.correlationId.toString)
      .build()
  }

  def parseItemProperties(properties: BasicProperties) : HyppoMessageProperties = {
    val replyQueue  = properties.getReplyTo
    val correlation = UUID.fromString(properties.getCorrelationId)
    val expiration  = Duration.ofMillis(properties.getExpiration.toLong)
    val startedAt   = ZonedDateTime.ofInstant(properties.getTimestamp.toInstant, UTCZoneId).toLocalDateTime
    HyppoMessageProperties(correlation, replyQueue, startedAt, expiration)
  }
}
