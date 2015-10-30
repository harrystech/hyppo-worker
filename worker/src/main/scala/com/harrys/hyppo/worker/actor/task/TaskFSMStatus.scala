package com.harrys.hyppo.worker.actor.task

/**
 * Created by jpetty on 10/29/15.
 */
sealed trait TaskFSMStatus

object TaskFSMStatus {
  final case object PreparingToStart extends TaskFSMStatus
  final case object PerformingOperation extends TaskFSMStatus
  final case object UploadingLogs extends TaskFSMStatus
}
