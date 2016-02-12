package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.actor.amqp.QueueDetails
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{PartialStatusUpdate, QueueStatusUpdate}
import com.harrys.hyppo.worker.api.proto.WorkResource
import com.harrys.hyppo.worker.scheduling.{ResourceQueueMetrics, WorkQueueMetrics}

/**
  * Created by jpetty on 2/12/16.
  */
trait QueueStatusTracker {
  def integrationQueueMetrics(): Seq[WorkQueueMetrics]

  def handleStatusUpdate(update: QueueStatusUpdate): Unit
  def handleStatusUpdate(update: PartialStatusUpdate): Unit

  def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit
  def resourceAcquisitionFailed(queue: String, resources: Seq[WorkResource], failure: WorkResource): Unit
}
