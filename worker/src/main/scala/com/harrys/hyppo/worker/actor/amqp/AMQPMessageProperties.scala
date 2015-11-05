package com.harrys.hyppo.worker.actor.amqp

import java.time._
import java.util.UUID

import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.WorkerResources.ThrottledWorkerResource
import com.rabbitmq.client.AMQP.BasicProperties

/**
 * Created by jpetty on 10/29/15.
 */
object AMQPMessageProperties {
  //  Header set on messages indicating java serialization
  final val SerializationMimeType = "application/x-java-serialized-object"

  def enqueueProperties(correlationId: UUID, replyToQueue: String, timeToLive: scala.concurrent.duration.FiniteDuration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(timeToLive))
  }

  def enqueueProperties(correlationId: UUID, replyToQueue: String, timeToLive: Duration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, TimeUtils.currentLocalDateTime(), timeToLive)
  }

  def enqueueProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, timeToLive: scala.concurrent.duration.FiniteDuration) : BasicProperties = {
    enqueueProperties(correlationId, replyToQueue, startedAt, TimeUtils.javaDuration(timeToLive))
  }

  def enqueueProperties(properties: HyppoMessageProperties) : BasicProperties = {
    enqueueProperties(properties.correlationId, properties.replyToQueue, properties.startedAt, properties.timeToLive)
  }

  def enqueueProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, timeToLive: Duration) : BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .expiration(timeToLive.toMillis.toString)
      .timestamp(TimeUtils.javaLegacyDate(startedAt))
      .replyTo(replyToQueue)
      .correlationId(correlationId.toString)
      .build()
  }


  def replyProperties(original: HyppoMessageProperties) : BasicProperties = {
    replyProperties(original, TimeUtils.currentLocalDateTime())
  }

  def replyProperties(original: HyppoMessageProperties, timestamp: LocalDateTime) : BasicProperties = {
    replyProperties(original.correlationId, timestamp)
  }

  def replyProperties(correlationId: UUID, timestamp: LocalDateTime) : BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .timestamp(TimeUtils.currentLegacyDate())
      .correlationId(correlationId.toString)
      .build()
  }

  def throttleTokenProperties(resource: ThrottledWorkerResource) : BasicProperties = {
    val timeout = Math.max(resource.throttleRate.toMillis, 1L)
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .timestamp(TimeUtils.currentLegacyDate())
      .expiration(timeout.toString)
      .build()
  }

  def parseItemProperties(properties: BasicProperties) : HyppoMessageProperties = {
    val replyQueue  = properties.getReplyTo
    val correlation = UUID.fromString(properties.getCorrelationId)
    val expiration  = Duration.ofMillis(properties.getExpiration.toLong)
    val startedAt   = TimeUtils.toLocalDateTime(properties.getTimestamp)
    HyppoMessageProperties(correlation, replyQueue, startedAt, expiration)
  }
}
