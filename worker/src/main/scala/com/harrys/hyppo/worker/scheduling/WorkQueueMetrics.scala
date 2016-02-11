package com.harrys.hyppo.worker.scheduling

import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails
import com.harrys.hyppo.worker.api.code.ExecutableIntegration

/**
  * Created by jpetty on 2/11/16.
  */
final case class WorkQueueMetrics(integration: ExecutableIntegration, details: SingleQueueDetails, resources: Seq[ResourceQueueMetrics]) {

}
