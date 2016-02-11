package com.harrys.hyppo.worker.scheduling

import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import scala.util.Random

/**
  * Created by jpetty on 2/11/16.
  */
trait PriorityOrdering extends Ordering[WorkQueueMetrics]

object ExpectedCompletionOrdering extends PriorityOrdering {

  override def compare(x: WorkQueueMetrics, y: WorkQueueMetrics): Int = {
    (estimatedCompletion(x) compareTo estimatedCompletion(y)) * -1
  }

  private def estimatedCompletion(queue: WorkQueueMetrics): Double = {
    if (queue.details.rate == 0.0) Double.PositiveInfinity else queue.details.ready.toDouble / queue.details.rate
  }
}

object IdleSinceMinuteOrdering extends PriorityOrdering {

  override def compare(x: WorkQueueMetrics, y: WorkQueueMetrics): Int = {
    idleSinceMinute(x) compareTo idleSinceMinute(y)
  }

  private def idleSinceMinute(queue: WorkQueueMetrics): Long = {
    queue.details.idleSince.truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC)
  }
}

object ShufflePriorityOrdering extends PriorityOrdering {
  override def compare(x: WorkQueueMetrics, y: WorkQueueMetrics): Int = Random.nextInt(3) - 1
}