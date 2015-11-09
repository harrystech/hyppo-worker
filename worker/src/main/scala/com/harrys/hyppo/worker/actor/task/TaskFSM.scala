package com.harrys.hyppo.worker.actor.task

import akka.actor._
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization}
import com.harrys.hyppo.worker.actor.queue.WorkQueueExecution
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent.{OperationLogUploaded, OperationResultAvailable, OperationStarting}
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.api.proto.{FailureResponse, WorkerResponse}
import com.thenewmotion.akka.rabbitmq.Channel

/**
 * Created by jpetty on 10/29/15.
 */
final class TaskFSM
(
  config:     WorkerConfig,
  execution:  WorkQueueExecution,
  commander:  ActorRef
) extends LoggingFSM[TaskFSMStatus, Unit] {

  when(PreparingToStart){
    case Event(OperationStarting, _) =>
      log.info("Commander began executing operation")
      goto(PerformingOperation)

    case Event(Terminated(actor), _) if actor.equals(commander)  =>
      log.warning("Commander exited before starting work. Requeueing operation")
      releaseWorkForRetry()
      stop(FSM.Shutdown)

    case Event(ImpendingShutdown, _) =>
      log.warning("Shutdown pending. Work will be sent back to queue")
      execution.tryWithChannel { c =>
        c.basicReject(execution.headers.deliveryTag, true)
        execution.leases.releaseAll()
      }
      context.unwatch(commander)
      stop(FSM.Shutdown)
  }

  when(PerformingOperation){
    case Event(OperationResultAvailable(fail: FailureResponse), _) =>
      val summary = fail.exception.map(_.summary).getOrElse("<unknown failure>")
      log.error(s"Operation failed. Sending response to results queue. Failure: ${ summary }")
      completeWithResponse(fail)
      goto(UploadingLogs)

    case Event(OperationResultAvailable(response), _) =>
      log.info("Work results produced successfully. Awaiting log upload.")
      completeWithResponse(response)
      goto(UploadingLogs)

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.error("Unexpected commander termination while operations were executing")
      execution.leases.tryReleaseAll()
      stop(FSM.Failure("Commander actor died during execution"))

    case Event(ImpendingShutdown, _) =>
      context.unwatch(commander)
      if (execution.idempotent) {
        log.warning("Shutdown in progress. Idempotent work is assumed safe and will be sent back to queue")
        releaseWorkForRetry()
        stop(FSM.Shutdown)
      } else {
        log.warning("Shutdown in progress. Unsafe work will be marked failed")
        stop(FSM.Shutdown)
      }

  }

  when(UploadingLogs) {
    case Event(OperationLogUploaded, _) =>
      log.info(s"Work log item successfully uploaded")
      taskFullyCompleted()

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.warning(s"Commander terminated during log uploads")
      taskFullyCompleted()

    case Event(ImpendingShutdown, _) =>
      log.warning(s"Shutdown pending. Work log uploading failed")
      taskFullyCompleted()
  }

  onTransition {
    //  When dealing with unsafe operations, the ACK must happen immediately before
    // the work is started in the executor
    case PreparingToStart -> PerformingOperation if !execution.idempotent =>
      execution.withChannel(_.basicAck(execution.headers.deliveryTag, false))

    //  When dealing with idempotent operations, the ACK is safe to defer until
    //  after results are produced, since no retries are safe
    case PerformingOperation -> UploadingLogs if execution.idempotent =>
      execution.withChannel(_.basicAck(execution.headers.deliveryTag, false))

    //  Always log transition details
    case from -> to => logTransitionDetails(from, to)
  }

  onTermination {
    case StopEvent(reason, _, _) =>
      log.info(s"Stopped ${execution.input.summaryString} : ${ reason.toString }")
  }

  private val serialization = new AMQPSerialization(context)
  startWith(PreparingToStart, Nil)
  initialize()
  //  The TaskFSM needs notification if the commander dies
  context.watch(commander)

  //  AND GO
  commander ! execution.input

  //
  // Begin Helpers
  //

  def logTransitionDetails(from: TaskFSMStatus, to: TaskFSMStatus): Unit = {
    log.debug(s"${ execution.input.summaryString } - ${ from } => ${ to }")
  }

  def releaseWorkForRetry() : Unit = {
    execution.withChannel { channel =>
      channel.basicReject(execution.headers.deliveryTag, true)
      execution.leases.releaseAll()
    }
  }

  def completeWithResponse(response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    val props = AMQPMessageProperties.replyProperties(execution.headers)
    execution.withChannel { channel =>
      channel.basicPublish("", execution. headers.replyToQueue, true, false, props, body)
      execution.leases.releaseAll()
    }
  }

  def taskFullyCompleted() : State = {
    context.unwatch(commander)
    stop(FSM.Normal)
  }

  def sendAckForItem(channel: Channel): Unit = {
    channel.basicAck(execution.headers.deliveryTag, false)
  }

  def publishWorkResponse(channel: Channel, response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    val props = AMQPMessageProperties.replyProperties(execution.headers)
    log.debug(s"Publishing to queue ${ execution.headers.replyToQueue } : ${ response.toString }")
    channel.basicPublish("", execution.headers.replyToQueue, true, false, props, body)
  }
}
