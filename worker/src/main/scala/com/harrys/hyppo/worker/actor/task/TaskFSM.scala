package com.harrys.hyppo.worker.actor.task

import akka.actor.{ActorRef, FSM, LoggingFSM, Terminated}
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.worker.actor.amqp.{AMQPSerialization, RabbitQueueItem, WorkQueueItem}
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent.{OperationLogUploaded, OperationResultAvailable, OperationStarting}
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.api.proto.{FailureResponse, PersistProcessedDataRequest, WorkerResponse}

/**
 * Created by jpetty on 10/29/15.
 */
final class TaskFSM
(
  config:     WorkerConfig,
  item:       WorkQueueItem,
  commander:  ActorRef
) extends LoggingFSM[TaskFSMStatus, ResultLogData] {

  when(PreparingToStart){
    case Event(OperationStarting, data) =>
      log.info("Commander began executing operation")
      goto(PerformingOperation) using data

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.warning("Commander exited before starting work. Requeueing operation")
      workShouldRetry(FSM.Failure("Commander actor died during execution"))

    case Event(ImpendingShutdown, _) =>
      log.warning("Shutdown pending. Work will be sent back to queue")
      workShouldRetry(FSM.Shutdown)
  }

  when(PerformingOperation){
    case Event(OperationResultAvailable(fail: FailureResponse), data) =>
      val summary = fail.exception.map(_.summary).getOrElse("<unknown failure>")
      log.error(s"Operation failed. Sending response to results queue and exiting. Failure: ${ summary }. ")
      publishWorkResponse(fail)
      sendAckForItem()
      //  This resets the operation state so that it's 1 log upload away from completing
      val next = data.copy(resultsSent = data.subTasksTotal, logsUploaded = data.subTasksTotal - 1)
      goto(UploadingLogs) using next

    case Event(OperationResultAvailable(response), data) =>
      publishWorkResponse(response)
      val next = data.newSentResult
      if (next.hasMoreResultsPending){
        log.info(s"Operation completed normally. ${ next.pendingResults } operations still remain")
        stay() using next
      } else if (next.hasMoreLogsPending) {
        log.info("All results completed successfully. Awaiting final log upload to acknowledge work completion.")
        waitForLogUploads() using next
      } else {
        log.info("All work has completed successfully")
        taskFullyCompleted()
      }

    case Event(OperationLogUploaded, data) =>
      val next = data.newUploadedLog
      if (next.hasMoreResultsPending){
        log.info(s"Operation completed normally. ${ next.pendingResults } operations still remain")
        stay() using next
      } else if (next.hasMoreLogsPending) {
        log.info("All results completed successfully. Awaiting final log upload to acknowledge work completion.")
        waitForLogUploads() using next
      } else {
        log.info("All work has completed successfully")
        taskFullyCompleted()
      }

    case Event(Terminated(actor), data) if actor.equals(commander) =>
      log.error(s"Unexpected termination while commander was running. ${ data.pendingResults } operations were still pending.")
      workShouldRetry(FSM.Failure("Commander actor died during execution"))

    case Event(ImpendingShutdown, _) =>
      if (idempotent) {
        log.warning("Shutdown in progress. Idempotent work is assumed safe and will be sent back to queue")
        workShouldRetry(FSM.Shutdown)
      } else {
        log.warning("Shutdown in progress. Unsafe work will be marked failed")
        stop(FSM.Shutdown)
      }

  }

  when(UploadingLogs) {
    case Event(OperationLogUploaded, data) =>
      val next = data.newUploadedLog
      if (next.hasMoreLogsPending){
        log.info(s"Work log item successfully uploaded. ${ next.pendingUploads } logs are still outstanding")
        stay() using next
      } else {
        log.info("All work completed and logs uploaded successfully")
        taskFullyCompleted()
      }

    case Event(Terminated(actor), data) if actor.equals(commander) =>
      if (data.hasMoreLogsPending){
        log.warning(s"Commander terminated during log uploads. ${ data.pendingUploads } log uploads failed to complete")
      } else {
        log.info("All work completed and logs uploaded successfully")
      }
      taskFullyCompleted()

    case Event(ImpendingShutdown, data) =>
      log.warning(s"Shutdown pending. ${ data.pendingUploads } log uploads failed")
      taskFullyCompleted()
  }



  val idempotent: Boolean = item.input match {
    case p: PersistProcessedDataRequest if p.integration.details.persistingSemantics == PersistingSemantics.Unsafe =>
      log.debug(s"Task ${ p.summaryString } is not idempotent and will assume aggressive ACK behaviors")
      false
    case _ =>
      log.debug(s"Task ${ item.input.summaryString } is idempotent and will use lazy ACK behaviors")
      false
  }

  onTransition {
    //  When dealing with unsafe operations, the ACK must happen immediately before
    // the work is started in the executor
    case PreparingToStart -> PerformingOperation if !idempotent => sendAckForItem()
    //  Always log transition details
    case from -> to => logTransitionDetails(from, to)
  }

  onTermination {
    case StopEvent(reason, _, _) =>
      log.info(s"Stopped ${item.input.summaryString} : ${ reason.toString }")
  }

  private val serialization = new AMQPSerialization(context)
  private var hasSentAck    = false

  startWith(PreparingToStart, ResultLogData(item.input.subtaskCount))
  initialize()
  //  AND GO
  commander ! item.input

  //
  // Begin Helpers
  //

  def rabbit: RabbitQueueItem = item.rabbitItem

  def logTransitionDetails(from: TaskFSMStatus, to: TaskFSMStatus): Unit = {
    log.debug(s"${ item.input.summaryString } - ${ from }(${ stateData.inspect }) => ${ to }(${ nextStateData.inspect })")
  }

  def waitForLogUploads() : State = {
    sendAckForItem()
    goto(UploadingLogs)
  }

  def taskFullyCompleted() : State = {
    sendAckForItem()
    context.unwatch(commander)
    stop(FSM.Normal)
  }

  def workShouldRetry(reason: FSM.Reason) : State = {
    sendRejectionForItem(requeue = true)
    context.unwatch(commander)
    stop(reason)
  }

  def sendAckForItem(): Unit = if (!hasSentAck) {
    //  Acks only fire once.
    hasSentAck = true
    rabbit.sendAck()
  }

  def sendRejectionForItem(requeue: Boolean = true) : Unit = if (!hasSentAck) {
    //  Acks only fire once.
    hasSentAck = true
    rabbit.sendReject()
  }

  def publishWorkResponse(response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    log.debug(s"Publishing to queue ${ rabbit.replyToQueue } : ${ response.toString }")
    rabbit.publishReply(body)
  }
}
