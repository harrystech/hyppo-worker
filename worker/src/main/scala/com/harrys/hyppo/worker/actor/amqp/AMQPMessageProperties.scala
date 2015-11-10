package com.harrys.hyppo.worker.actor.amqp

import java.time._
import java.util.UUID

import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.queue.QueueItemHeaders
import com.harrys.hyppo.worker.api.proto.ThrottledWorkResource
import com.rabbitmq.client.AMQP.BasicProperties

/**
 * Created by jpetty on 10/29/15.
 */
object AMQPMessageProperties {
  //  Header set on messages indicating java serialization
  final val SerializationMimeType = "application/x-java-serialized-object"

  def enqueueProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, timeToLive: Duration) : BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .expiration(timeToLive.toMillis.toString)
      .timestamp(TimeUtils.javaLegacyDate(startedAt))
      .replyTo(replyToQueue)
      .correlationId(correlationId.toString)
      .build()
  }


  def replyProperties(original: QueueItemHeaders): BasicProperties = {
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .timestamp(TimeUtils.currentLegacyDate())
      .correlationId(original.correlationId)
      .build()
  }

  def throttleTokenProperties(resource: ThrottledWorkResource) : BasicProperties = {
    val timeout = Math.max(resource.throttleRate.toMillis, 1L)
    new BasicProperties.Builder()
      .contentType(SerializationMimeType)
      .timestamp(TimeUtils.currentLegacyDate())
      .expiration(timeout.toString)
      .build()
  }
}
