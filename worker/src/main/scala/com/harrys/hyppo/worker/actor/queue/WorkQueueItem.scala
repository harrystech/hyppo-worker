package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.api.proto.WorkerInput

/**
  * Created by jpetty on 11/6/15.
  */
final case class WorkQueueItem(headers: QueueItemHeaders, input: WorkerInput) {

}
