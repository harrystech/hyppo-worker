package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import akka.actor.{ActorRef, ActorLogging, Actor}
import com.google.inject.{Injector, Provider}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.harrys.hyppo.worker.scheduling.WorkQueuePrioritizer
import com.rabbitmq.client.Channel
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec

/**
  * Created by jpetty on 2/12/16.
  */
final class PluggableDelegation @Inject()
(
  injectorProvider:   Provider[Injector],
  config:             WorkerConfig,
  statusTracker:      QueueStatusTracker,
  prioritizer:        WorkQueuePrioritizer,
  strategyFactory:    DelegationStrategy.Factory,
  statusPollingActorFactory: RabbitQueueStatusActor.Factory
) extends Actor with ActorLogging with ActorInject {

  override def injector: Injector = injectorProvider.get()

  //  Helper objects for dealing with queues
  val serializer    = new AMQPSerialization(config.secretKey)
  val resources     = new ResourceLeasing
  //  The current known information about the queues, updated via assignment once fetched
  var currentStats  = Map[String, SingleQueueDetails]()

  //  Actor that sends status update events
  val statusActor   = injectActor(statusPollingActorFactory(self), "queue-status")
  val strategy      = strategyFactory(statusTracker, prioritizer)

  private final case class SyncAction(action: () => Unit)

  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  override def receive: Receive = {
    case RequestForPreferredWork(channelActor, prefer) =>
      val worker   = sender()
      val ordering = strategy.priorityOrderWithPreference(prefer)
      if (ordering.hasNext) {
        channelActor ! ChannelMessage(c => performDelegationSequence(c, worker, ordering))
      } else {
        log.debug("No work is currently available in any queue. Ignoring request from {} with work preference {}", worker, prefer)
      }

    case RequestForAnyWork(channelActor) =>
      val worker   = sender()
      val ordering = strategy.priorityOrderWithoutAffinity()
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

    case SyncAction(action) =>
      action()

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.stop(statusActor)
      context.become(shutdownImminent, discardOld = false)
  }


  @tailrec
  def performDelegationSequence(channel: Channel, worker: ActorRef, checkOrdering: Iterator[String]): Unit = {
    if (checkOrdering.hasNext) {
      val queue = checkOrdering.next()
      tryExecutionAcquisition(channel, worker, queue) match {
        case Some(execution) =>
          log.debug(s"Successfully acquired task execution: ${ execution.input.summaryString }")
          worker ! execution
        case None => performDelegationSequence(channel, worker, checkOrdering)
      }
    }
  }

  def tryExecutionAcquisition(channel: Channel, worker: ActorRef, queue: String): Option[WorkQueueExecution] = {
    dequeueWithoutAck(channel, queue).flatMap { item =>
      resources.leaseResources(channel, item.input.resources) match {
        case Left(leases) =>
          val execution = WorkQueueExecution(channel, item.headers, item.input, leases)
          self ! SyncAction(() => statusTracker.resourcesAcquiredSuccessfully(queue, item.input.resources))
          Some(execution)
        case Right(ResourceUnavailable(unavailable)) =>
          log.info(s"Unable to acquire ${ unavailable.inspect } to perform ${ item.input.summaryString }. Sending back to queue.")
          channel.basicReject(item.headers.deliveryTag, true)
          self ! SyncAction(() => statusTracker.resourceAcquisitionFailed(queue, item.input.resources, unavailable))
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