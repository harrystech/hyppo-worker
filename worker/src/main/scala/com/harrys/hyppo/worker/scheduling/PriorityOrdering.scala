package com.harrys.hyppo.worker.scheduling

import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails

import scala.util.Random

/**
  * Created by jpetty on 2/11/16.
  */
trait PriorityOrdering extends Ordering[SingleQueueDetails]

case object ExpectedCompletionOrdering extends PriorityOrdering {

  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = {
    (estimatedCompletion(x) compareTo estimatedCompletion(y)) * -1
  }

  private def estimatedCompletion(details: SingleQueueDetails): Double = {
    if (details.rate == 0.0) Double.PositiveInfinity else details.ready.toDouble / details.rate
  }

  override def toString: String = this.productPrefix
}

case object IdleSinceMinuteOrdering extends PriorityOrdering {

  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = {
    idleSinceMinute(x) compareTo idleSinceMinute(y)
  }

  private def idleSinceMinute(details: SingleQueueDetails): Long = {
    details.idleSince.truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC)
  }

  override def toString: String = this.productPrefix
}

case object AbsoluteSizeOrdering extends PriorityOrdering {
  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = x.size compareTo y.size
  override def toString: String = this.productPrefix
}

case object ShuffleOrdering extends PriorityOrdering {
  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = Random.nextInt(3) - 1
  override def toString: String = this.productPrefix
}