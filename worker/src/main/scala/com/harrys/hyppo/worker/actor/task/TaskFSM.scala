package com.harrys.hyppo.worker.actor.task

import akka.actor._
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization}
import com.harrys.hyppo.worker.actor.queue.WorkQueueExecution
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent.{OperationLogUploaded, OperationResponseAvailable, OperationStarting}
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.api.proto.{FailureResponse, WorkerResponse}
import com.thenewmotion.akka.rabbitmq.Channel

import scala.util.Try

/**
 * Created by jpetty on 10/29/15.
 */
final class TaskFSM
(
  config:     WorkerConfig,
  execution:  WorkQueueExecution,
  commander:  ActorRef
) extends FSM[TaskFSMStatus, Unit] with ActorLogging {

  when(PreparingToStart){
    case Event(OperationStarting, _) =>
      log.debug(s"Commander began running execution ${ execution.input.summaryString }")
      goto(PerformingOperation)

    case Event(Terminated(actor), _) if actor.equals(commander)  =>
      log.warning(s"Commander exited before starting ${ execution.input.summaryString }. Requeueing operation.")
      releaseWorkForRetry()
      stop(FSM.Shutdown)

    case Event(ImpendingShutdown, _) =>
      log.warning(s"Shutdown pending. ${ execution.input.summaryString } be sent back to queue")
      execution.tryWithChannel { c =>
        c.basicReject(execution.headers.deliveryTag, true)
        execution.leases.releaseAll()
      }
      context.unwatch(commander)
      stop(FSM.Shutdown)
  }

  when(PerformingOperation){
    case Event(OperationResponseAvailable(fail: FailureResponse), _) =>
      val summary = fail.exception.map(_.summary).getOrElse("<unknown failure>")
      log.error(s"${ execution.input.summaryString } failed. Sending response to results queue. Failure: ${ summary }")
      completeWithResponse(fail)
      goto(UploadingLogs)

    case Event(OperationResponseAvailable(response), _) =>
      log.debug(s"${ execution.input.summaryString } produced results successfully")
      completeWithResponse(response)
      goto(UploadingLogs)

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.error(s"Unexpected commander termination while executing: ${ execution.input.summaryString }")
      Try(execution.leases.releaseAll())
      stop(FSM.Failure("Commander actor terminated unexpectedly."))

    case Event(ImpendingShutdown, _) =>
      context.unwatch(commander)
      if (execution.idempotent) {
        log.warning(s"Shutdown in progress. ${ execution.input.summaryString } is idempotent and will be sent back to queue")
        releaseWorkForRetry()
        stop(FSM.Shutdown)
      } else {
        log.warning(s"Shutdown in progress. ${ execution.input.summaryString } is unsafe to re-execute and will be marked failed")
        stop(FSM.Shutdown)
      }

  }

  when(UploadingLogs) {
    case Event(OperationLogUploaded, _) =>
      log.debug(s"Log upload for ${ execution.input.summaryString } completed successfully")
      taskFullyCompleted()

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.warning(s"Commander terminated while before logs could be uploaded for ${ execution.input.summaryString }")
      taskFullyCompleted()

    case Event(ImpendingShutdown, _) =>
      log.warning(s"Shutdown in progress interrupted log uploading for ${ execution.input.summaryString }")
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
    case StopEvent(reason, state, _) =>
      log.debug(s"Stopped ${execution.input.summaryString} :: ${ state } - ${ reason.toString }")
  }

  private val serialization = new AMQPSerialization(config.secretKey)
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
    log.debug(s"Sending RabbitMQ ACK for ${ execution.input.summaryString }")
    channel.basicAck(execution.headers.deliveryTag, false)
  }

  def publishWorkResponse(channel: Channel, response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    val props = AMQPMessageProperties.replyProperties(execution.headers)
    log.debug(s"Publishing response for ${ execution.input.summaryString } to queue ${ execution.headers.replyToQueue } : ${ response.toString }")
    channel.basicPublish("", execution.headers.replyToQueue, true, false, props, body)
  }
}
