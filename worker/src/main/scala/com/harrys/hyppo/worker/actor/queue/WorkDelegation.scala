package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.google.inject.{Injector, Provider}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.api.proto.{WorkResource, WorkerInput}
import com.rabbitmq.client.Channel
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec

/**
  * Created by jpetty on 2/12/16.
  */
final class WorkDelegation @Inject()
(
  injectorProvider:   Provider[Injector],
  val config:         WorkerConfig,
  val statusTracker:  QueueMetricsTracker,
  val strategy:       DelegationStrategy,
  statusPollingActorFactory: RabbitQueueStatusActor.Factory
) extends Actor with ActorLogging with ActorInject {

  override def injector: Injector = injectorProvider.get()

  //  Helper objects for dealing with queues
  val serializer    = new AMQPSerialization(config.secretKey)
  val leasing       = new ResourceLeasing()

  //  Actor that sends status update events
  val statusActor   = injectActor(statusPollingActorFactory(self), "queue-status")

  //  Pinged back from the worker channel when the acquisition fails. This is to avoid concurrency race conditions
  //  since resource leasing happens inside the worker channel's thread and not necessarily the delegation thread.
  private final case class ResourceStatusSync(queue: String, resources: Seq[WorkResource], failed: Option[WorkResource])

  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  override def receive: Receive = {
    case RequestForPreferredWork(channelActor, prefer) =>
      val worker   = sender()
      val ordering = strategy.priorityOrderWithPreference(prefer, statusTracker.generalQueueMetrics(), statusTracker.integrationQueueMetrics())
      if (ordering.hasNext) {
        channelActor ! ChannelMessage(c => performDelegationSequence(c, worker, ordering))
      } else {
        log.debug("No work is currently available in any queue. Ignoring request from {} with work preference {}", worker, prefer.printableName)
      }

    case RequestForAnyWork(channelActor) =>
      val worker   = sender()
      val ordering = strategy.priorityOrderWithoutAffinity(statusTracker.generalQueueMetrics(), statusTracker.integrationQueueMetrics())
      if (ordering.hasNext) {
        channelActor ! ChannelMessage(c => performDelegationSequence(c, worker, ordering))
      } else {
        log.debug("No work is currently available in any queue. Ignoring request from {} with no work preference", worker)
      }

    case update: RabbitQueueStatusActor.QueueStatusUpdate =>
      log.debug("Received new full queue status update")
      statusTracker.handleStatusUpdate(update)

    case partial: RabbitQueueStatusActor.PartialStatusUpdate =>
      log.debug(s"Received incremental queue status update for {} to size {}", partial.name, partial.size)
      statusTracker.handleStatusUpdate(partial)

    case ResourceStatusSync(queue, resources, Some(failed)) =>
      log.debug("Failed to acquire resource {} for work queue {}", failed.inspect, queue)
      statusTracker.resourceAcquisitionFailed(queue, resources, failed)

    case ResourceStatusSync(queue, resources, None) =>
      log.debug("Successfully acquired resources {} for work queue {}", resources.view.map(_.resourceName))
      statusTracker.resourcesAcquiredSuccessfully(queue, resources)

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.stop(statusActor)
      context.become(shutdownImminent, discardOld = false)
  }


  @tailrec
  def performDelegationSequence(channel: Channel, worker: ActorRef, checkOrdering: Iterator[SingleQueueDetails]): Unit = {
    if (checkOrdering.hasNext) {
      val queue = checkOrdering.next()
      tryExecutionAcquisition(channel, worker, queue) match {
        case Some(execution) =>
          log.debug("Successfully acquired task execution {} for worker {}", execution.input.summaryString, worker)
          worker ! execution
        case None => performDelegationSequence(channel, worker, checkOrdering)
      }
    }
  }

  def tryExecutionAcquisition(channel: Channel, worker: ActorRef, queue: SingleQueueDetails): Option[WorkQueueExecution] = {
    dequeueWithoutAck(channel, queue.queueName).flatMap { item =>
      leasing.leaseResources(channel, item.input.resources) match {
        case Left(leases) =>
          val execution = WorkQueueExecution(channel, item.headers, item.input, leases)
          self ! ResourceStatusSync(queue.queueName, item.input.resources, None)
          Some(execution)
        case Right(ResourceUnavailable(unavailable)) =>
          log.info("Unable to acquire {} to perform {}. Sending back to queue.", unavailable.inspect, item.input.summaryString)
          self ! ResourceStatusSync(queue.queueName, item.input.resources, Some(unavailable))
          channel.basicReject(item.headers.deliveryTag, true)
          None
      }
    }
  }


  def dequeueWithoutAck(channel: Channel, queueName: String): Option[WorkQueueItem] = {
    val response = channel.basicGet(queueName, false)
    if (response == null){
      self ! RabbitQueueStatusActor.PartialStatusUpdate(queueName, 0)
      None
    } else {
      self ! RabbitQueueStatusActor.PartialStatusUpdate(queueName, response.getMessageCount)
      val workerInput = serializer.deserialize[WorkerInput](response.getBody)
      val itemHeaders = new QueueItemHeaders(response.getEnvelope, response.getProps)
      Some(WorkQueueItem(itemHeaders, workerInput))
    }
  }
}