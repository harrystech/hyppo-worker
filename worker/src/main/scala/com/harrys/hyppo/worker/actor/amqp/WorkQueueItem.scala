package com.harrys.hyppo.worker.actor.amqp

import com.harrys.hyppo.worker.api.proto.WorkerInput

/**
 * Created by jpetty on 9/21/15.
 */
final case class WorkQueueItem(rabbitItem: RabbitQueueItem, input: WorkerInput) {

}
