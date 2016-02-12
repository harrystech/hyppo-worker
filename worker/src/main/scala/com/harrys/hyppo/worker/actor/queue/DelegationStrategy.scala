package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{QueueStatusUpdate, PartialStatusUpdate}
import com.harrys.hyppo.worker.actor.amqp.{QueueDetails, SingleQueueDetails}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.WorkResource
import com.harrys.hyppo.worker.scheduling.{WorkQueuePrioritizer, WorkQueueMetrics}
import com.rabbitmq.client.Channel

/**
  * Created by jpetty on 2/12/16.
  */
trait DelegationStrategy {
  def priorityOrderWithoutAffinity(): Iterator[String]
  def priorityOrderWithPreference(prefer: ExecutableIntegration): Iterator[String]
}

object DelegationStrategy {
  trait Factory {
    def apply(@Assisted statusTracker: QueueStatusTracker, @Assisted prioritizer: WorkQueuePrioritizer): DelegationStrategy
  }
}

final class DefaultDelegationStrategy @Inject()
(
  @Assisted statusTracker:   QueueStatusTracker,
  @Assisted workPrioritizer: WorkQueuePrioritizer
) extends DelegationStrategy {

  override def priorityOrderWithoutAffinity(): Iterator[String] = ???

  override def priorityOrderWithPreference(prefer: ExecutableIntegration): Iterator[String] = ???
}
