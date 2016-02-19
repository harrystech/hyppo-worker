package com.harrys.hyppo.worker.scheduling

import java.time.Instant

import com.harrys.hyppo.worker.actor.amqp.QueueDetails
import com.harrys.hyppo.worker.api.proto.WorkResource

/**
  * Created by jpetty on 2/11/16.
  */
final case class ResourceQueueMetrics(resource: WorkResource, details: QueueDetails, timeOfLastContention: Option[Instant]) {
  def isUnderContention: Boolean = timeOfLastContention.isDefined
}
