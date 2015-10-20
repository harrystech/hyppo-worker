package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.actor.amqp.QueueStatusInfo
import com.harrys.hyppo.worker.api.proto.WorkerInput

/**
 * Created by jpetty on 9/28/15.
 */
trait WorkDispatcher {

  def enqueue(input: WorkerInput) : Unit

  def fetchQueueStatuses() : Seq[QueueStatusInfo]

}
