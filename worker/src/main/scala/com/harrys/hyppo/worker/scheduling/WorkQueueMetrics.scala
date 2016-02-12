package com.harrys.hyppo.worker.scheduling

import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails

/**
  * Created by jpetty on 2/11/16.
  */
final case class WorkQueueMetrics(details: SingleQueueDetails, resources: Seq[ResourceQueueMetrics]) {
  def hasWork: Boolean = details.hasWork
}
