package com.harrys.hyppo.worker.scheduling

import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails

import scala.util.Random

/**
  * Created by jpetty on 2/11/16.
  */
trait PriorityOrdering extends Ordering[SingleQueueDetails]

object ExpectedCompletionOrdering extends PriorityOrdering {

  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = {
    (estimatedCompletion(x) compareTo estimatedCompletion(y)) * -1
  }

  private def estimatedCompletion(details: SingleQueueDetails): Double = {
    if (details.rate == 0.0) Double.PositiveInfinity else details.ready.toDouble / details.rate
  }
}

object IdleSinceMinuteOrdering extends PriorityOrdering {

  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = {
    idleSinceMinute(x) compareTo idleSinceMinute(y)
  }

  private def idleSinceMinute(details: SingleQueueDetails): Long = {
    details.idleSince.truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC)
  }
}

object ShufflePriorityOrdering extends PriorityOrdering {
  override def compare(x: SingleQueueDetails, y: SingleQueueDetails): Int = Random.nextInt(3) - 1
}