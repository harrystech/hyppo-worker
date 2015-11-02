package com.harrys.hyppo.worker.actor.amqp

import java.time.{Duration, LocalDateTime}
import java.util.UUID

/**
 * Created by jpetty on 11/2/15.
 */
final case class HyppoMessageProperties(correlationId: UUID, replyToQueue: String, startedAt: LocalDateTime, timeToLive: Duration) {
  def expiredAt: LocalDateTime = startedAt.plus(timeToLive)

  def isExpired: Boolean = {
    expiredAt.isBefore(AMQPMessageProperties.currentLocalDateTime)
  }
  def workAge: Duration = Duration.between(startedAt, AMQPMessageProperties.currentLocalDateTime)
}
