package com.harrys.hyppo.worker.actor.task

import akka.actor._
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.worker.actor.amqp.{QueueHelpers, AMQPMessageProperties, QueueNaming, AMQPSerialization}
import com.harrys.hyppo.worker.actor.queue.{ResourceManagement, WorkQueueExecution}
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent.{OperationLogUploaded, OperationResultAvailable, OperationStarting}
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.api.proto.{FailureResponse, PersistProcessedDataRequest, WorkerResponse}
import com.thenewmotion.akka.rabbitmq.{Channel, ChannelMessage}

/**
 * Created by jpetty on 10/29/15.
 */
final class TaskFSM
(
  config:     WorkerConfig,
  execution:  WorkQueueExecution,
  commander:  ActorRef
) extends LoggingFSM[TaskFSMStatus, Unit] {

  /**
    * TODO: All this shit is fucked now that the channel adapter can interrupt it. The handling of channel death will
    * vary based on phase of work and idempotence flag. If the work is not idempotent and the channel closes, we can try
    * to re-create a new channel from the worker config and publish the result that way.
    *
    * FIX IT ON MONDAY
    */

  when(PreparingToStart){
    case Event(OperationStarting, data) =>
      log.info("Commander began executing operation")
      goto(PerformingOperation)

    case Event(Terminated(actor), _) if actor.equals(commander)  =>
      log.warning("Commander exited before starting work. Requeueing operation")
      workShouldRetry(FSM.Failure("Commander actor died before execution"))

    case Event(Terminated(actor), _) if actor.equals(channelActor) =>
      log.warning("Queue channel actor died before starting work. Work is automatically requeued, but commander must terminate")
      context.unwatch(commander)

      workShouldRetry(FSM.Failure("Queue actor died before execution"))

    case Event(ImpendingShutdown, _) =>
      log.warning("Shutdown pending. Work will be sent back to queue")
      workShouldRetry(FSM.Shutdown)
  }

  when(PerformingOperation){
    case Event(OperationResultAvailable(fail: FailureResponse), data) =>
      val summary = fail.exception.map(_.summary).getOrElse("<unknown failure>")
      log.error(s"Operation failed. Sending response to results queue. Failure: ${ summary }. ")
      waitForLogUploads(fail)

    case Event(OperationResultAvailable(response), data) =>
      log.info("Work results produced successfully. Awaiting log upload.")
      waitForLogUploads(response)

    case Event(Terminated(actor), data) if actor.equals(commander) =>
      log.error("Unexpected termination while operations were still executing")
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
      log.info(s"Work log item successfully uploaded")
      taskFullyCompleted()

    case Event(Terminated(actor), data) if actor.equals(commander) =>
      log.warning(s"Commander terminated during log uploads")
      taskFullyCompleted()

    case Event(ImpendingShutdown, data) =>
      log.warning(s"Shutdown pending. Work log uploading failed")
      taskFullyCompleted()
  }



  val idempotent: Boolean = execution.input match {
    case p: PersistProcessedDataRequest if p.integration.details.persistingSemantics == PersistingSemantics.Unsafe =>
      log.debug(s"Task ${ p.summaryString } is not idempotent and will assume aggressive ACK behaviors")
      false
    case _ =>
      log.debug(s"Task ${ execution.input.summaryString } is idempotent and will use lazy ACK behaviors")
      false
  }

  onTransition {
    //  When dealing with unsafe operations, the ACK must happen immediately before
    // the work is started in the executor
    case PreparingToStart -> PerformingOperation if !idempotent =>
      channelActor ! ChannelMessage(c => sendAckForItem(c))
    //  Always log transition details
    case from -> to => logTransitionDetails(from, to)
  }

  onTermination {
    case StopEvent(reason, _, _) =>
      log.info(s"Stopped ${execution.input.summaryString} : ${ reason.toString }")
  }

  private val serialization = new AMQPSerialization(context)
  private val resources     = new ResourceManagement
  private val channelActor  = execution.channelActor
  private var hasSentAck    = false
  //  The TaskFSM takes over from the WorkerFSM on channel ownership after creation.
  context.watch(execution.channelActor)

  startWith(PreparingToStart, Nil)
  initialize()
  //  AND GO
  commander ! execution.input

  //
  // Begin Helpers
  //

  def logTransitionDetails(from: TaskFSMStatus, to: TaskFSMStatus): Unit = {
    log.debug(s"${ execution.input.summaryString } - ${ from } => ${ to }")
  }

  def waitForLogUploads(response: WorkerResponse) : State = {
    //  This message is droppable for idempotent work, since no connection means it's already happening again.
    val allowDrops = !idempotent

    context.unwatch(channelActor)
    //  Publish response, ack message, release all held resources
    channelActor ! ChannelMessage(c => {
      publishWorkResponse(c, response)
      sendAckForItem(c)
      resources.releaseResources(c, execution.leases)
    }, dropIfNoChannel = allowDrops)

    //  The channel is now released from duty
    channelActor ! PoisonPill

    goto(UploadingLogs)
  }

  def taskFullyCompleted() : State = {
    context.unwatch(commander)
    stop(FSM.Normal)
  }

  def workShouldRetry(reason: FSM.Reason) : State = {
    context.unwatch(channelActor)
    channelActor ! ChannelMessage(c => sendRejectionForItem(c, requeue = true))
    channelActor ! PoisonPill
    context.unwatch(commander)
    stop(reason)
  }

  def sendAckForItem(channel: Channel): Unit = if (!hasSentAck) {
    //  Acks only fire once.
    hasSentAck = true
    channel.basicAck(execution.headers.deliveryTag, false)
  }

  def sendRejectionForItem(channel: Channel, requeue: Boolean = true) : Unit = if (!hasSentAck) {
    //  Acks only fire once.
    hasSentAck = true
    channel.basicReject(execution.headers.deliveryTag, requeue)
  }

  def publishWorkResponse(channel: Channel, response: WorkerResponse) : Unit = {
    val body  = serialization.serialize(response)
    val props = AMQPMessageProperties.replyProperties(execution.headers)
    log.debug(s"Publishing to queue ${ execution.headers.replyToQueue } : ${ response.toString }")
    channel.basicPublish("", execution.headers.replyToQueue, true, false, props, body)
  }
}
