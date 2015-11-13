package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.actor.amqp.{QueueDetails, SingleQueueDetails}
import com.harrys.hyppo.worker.api.proto.{WorkResource, WorkerInput}

/**
 * Created by jpetty on 9/28/15.
 */
trait WorkDispatcher {

  def enqueue(input: WorkerInput) : Unit

  def fetchLogicalHyppoQueueDetails() : Seq[QueueDetails]

  def fetchRawHyppoQueueDetails() : Seq[SingleQueueDetails]
}
