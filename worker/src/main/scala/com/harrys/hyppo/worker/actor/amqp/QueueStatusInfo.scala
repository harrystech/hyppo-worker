package com.harrys.hyppo.worker.actor.amqp

import java.time.LocalDateTime

import scala.concurrent.duration._

/**
 * Created by jpetty on 9/15/15.
 */
final case class QueueStatusInfo(name: String, size: Int, rate: Double, idleSince: LocalDateTime) {

  def isEmpty: Boolean = size <= 0

  def estimatedCompletionTime: Duration = {
    if (rate == 0.0){
      Duration.Inf
    } else {
      Duration(size.toDouble / rate, SECONDS)
    }
  }
}
