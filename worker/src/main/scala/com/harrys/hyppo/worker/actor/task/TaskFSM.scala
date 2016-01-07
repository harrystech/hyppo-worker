package com.harrys.hyppo.worker.actor.task

import akka.actor._
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization}
import com.harrys.hyppo.worker.actor.queue.WorkQueueExecution
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent.{OperationLogUploaded, OperationResponseAvailable, OperationStarting, TaskDependencyFailure}
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

  private val serialization = new AMQPSerialization(config.secretKey)

  when(PreparingToStart){
    case Event(OperationStarting, _) =>
      log.debug("Commander began running execution {}", execution.input.summaryString)
      goto(PerformingOperation)

    case Event(TaskDependencyFailure(error), _) =>
      log.warning("Downloading task inputs failed for execution {} - {}", execution.input.summaryString, error)
      releaseWorkForRetry()
      context.unwatch(commander)
      stop(FSM.Shutdown)

    case Event(Terminated(actor), _) if actor.equals(commander)  =>
      log.warning("Commander exited before starting {}. Requeueing operation.", execution.input.summaryString)
      releaseWorkForRetry()
      context.unwatch(commander)
      stop(FSM.Shutdown)

    case Event(ImpendingShutdown, _) =>
      log.warning(s"Shutdown pending. ${ execution.input.summaryString } be sent back to queue")
      releaseWorkForRetry()
      context.unwatch(commander)
      stop(FSM.Shutdown)
  }

  when(PerformingOperation){
    case Event(OperationStarting, _) =>
      log.warning(s"Received unexpected duplicate {} message", OperationStarting.productPrefix)
      stay()

    case Event(OperationResponseAvailable(fail: FailureResponse), _) =>
      val summary = fail.exception.map(_.summary).getOrElse("<unknown failure>")
      log.error("{} failed. Sending response to results queue. Failure: {}", execution.input.summaryString, summary)
      completeWithResponse(fail)
      goto(UploadingLogs)

    case Event(OperationResponseAvailable(response), _) =>
      log.debug("{} produced results successfully", execution.input.summaryString)
      completeWithResponse(response)
      goto(UploadingLogs)

    case Event(Terminated(actor), _) if actor.equals(commander) =>
      log.error("Unexpected commander termination while executing: {}", execution.input.summaryString)
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
      log.debug("Log upload for {} completed successfully", execution.input.summaryString)
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
    case from -> to => log.debug("{} - {} => {}", execution.input.summaryString, from, to)
  }

  onTermination {
    case StopEvent(reason, state, _) =>
      log.debug("Stopped {} :: {} - {}", execution.input.summaryString, state, reason)
  }

  //  The TaskFSM needs notification if the commander dies
  context.watch(commander)
  startWith(PreparingToStart, Nil)
  initialize()

  //  AND GO
  commander ! execution.input

  //
  // Begin Helpers
  //

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
    log.debug("Sending RabbitMQ ACK for {}", execution.input.summaryString)
    channel.basicAck(execution.headers.deliveryTag, false)
  }

  def publishWorkResponse(channel: Channel, response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    val props = AMQPMessageProperties.replyProperties(execution.headers)
    log.debug("Publishing response for {} to queue {} : {}", execution.input.summaryString, execution.headers.replyToQueue, response)
    channel.basicPublish("", execution.headers.replyToQueue, true, false, props, body)
  }
}
