package com.harrys.hyppo.worker.actor.queue

import java.time.{Duration, LocalDateTime}

import com.harrys.hyppo.util.TimeUtils
import com.rabbitmq.client.{BasicProperties, Envelope}

/**
  * Created by jpetty on 11/6/15.
  */
final class QueueItemHeaders(envelope: Envelope, properties: BasicProperties) {
  def deliveryTag: Long      = envelope.getDeliveryTag
  def sourceExchange: String = envelope.getExchange
  def sourceQueue: String    = envelope.getRoutingKey
  def isRedeliver: Boolean   = envelope.isRedeliver

  def correlationId: String       = properties.getCorrelationId
  def replyToQueue: String        = properties.getReplyTo
  def enqueuedAt: LocalDateTime   = TimeUtils.toLocalDateTime(properties.getTimestamp)
  def timeToLive: Duration        = Duration.ofMillis(properties.getExpiration.toLong)
  def expiredAfter: LocalDateTime = enqueuedAt.plus(timeToLive)

  def isExpired: Boolean = expiredAfter.isBefore(TimeUtils.currentLocalDateTime())
  def timeSinceEnqueue: Duration = Duration.between(enqueuedAt, TimeUtils.currentLocalDateTime())
}
