package com.harrys.hyppo.worker.actor.task

import com.harrys.hyppo.worker.api.proto.WorkerResponse

/**
 * Created by jpetty on 10/29/15.
 */
sealed trait TaskFSMEvent

object TaskFSMEvent {

  final case object OperationStarting extends TaskFSMEvent
  final case class OperationResultAvailable(response: WorkerResponse) extends TaskFSMEvent
  final case object OperationLogUploaded extends TaskFSMEvent

}
