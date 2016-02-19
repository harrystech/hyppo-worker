package com.harrys.hyppo.worker.actor.queue

import com.google.inject.ImplementedBy
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{PartialStatusUpdate, QueueStatusUpdate}
import com.harrys.hyppo.worker.api.proto.WorkResource
import com.harrys.hyppo.worker.scheduling.WorkQueueMetrics

/**
  * Created by jpetty on 2/12/16.
  */
@ImplementedBy(classOf[DefaultQueueStatusTracker])
trait QueueStatusTracker {
  def generalQueueMetrics(): WorkQueueMetrics
  def integrationQueueMetrics(): Seq[WorkQueueMetrics]

  def handleStatusUpdate(update: QueueStatusUpdate): Unit
  def handleStatusUpdate(update: PartialStatusUpdate): Unit

  def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit
  def resourceAcquisitionFailed(queue: String, resources: Seq[WorkResource], failure: WorkResource): Unit
}
