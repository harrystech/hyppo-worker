package com.harrys.hyppo.worker.actor.queue

import akka.actor.ActorRef
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation.AcquiredResourceLeases
import com.harrys.hyppo.worker.api.proto.WorkerInput

/**
  * Created by jpetty on 11/6/15.
  */
final case class WorkQueueExecution(channelActor: ActorRef, headers: QueueItemHeaders, input: WorkerInput, leases: AcquiredResourceLeases) {

}
