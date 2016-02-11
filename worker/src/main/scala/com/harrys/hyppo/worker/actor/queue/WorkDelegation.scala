package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import akka.actor._
import com.google.inject.{Injector, Provider}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.harrys.hyppo.worker.scheduling.{WorkQueueMetrics, WorkQueuePrioritizer}
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec
import scala.util.Random

/**
  * Created by jpetty on 11/6/15.
  */
final class WorkDelegation @Inject()
(
  injectorProvider: Provider[Injector],
  config:   WorkerConfig,
  naming:   QueueNaming,
  helpers:  QueueHelpers,
  prioritizer: WorkQueuePrioritizer,
  statusFactory: RabbitQueueStatusActor.Factory
) extends Actor with ActorLogging with ActorInject {

  override def injector: Injector = injectorProvider.get()

  //  Helper objects for dealing with queues
  val serializer    = new AMQPSerialization(config.secretKey)
  val resources     = new ResourceLeasing
  //  The current known information about the queues, updated via assignment once fetched
  var currentStats  = Map[String, SingleQueueDetails]()

  //  Actor that sends status update events
  val statusActor   = injectActor(statusFactory(self), "queue-status")


  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  override def receive: Receive = {
    case RequestForPreferredWork(channelActor, prefer) =>
      val worker  = sender()
      val filter  = naming.belongsToIntegration(prefer) _
      val base    = List(naming.generalQueueName) ++ integrationQueueOrder.map(_.queueName)
      //  Moves all queues belong to this integration to the front of the list
      val (head, tail) = base.partition(name => filter(name))
      val queues  = head ++ tail
      channelActor ! ChannelMessage(c => delegateWorkFromQueues(c, worker, queues))

    case RequestForAnyWork(channelActor) =>
      val worker  = sender()
      val queues  = List(naming.generalQueueName) ++ integrationQueueOrder.map(_.queueName)
      channelActor ! ChannelMessage(c => delegateWorkFromQueues(c, worker, queues))

    case RabbitQueueStatusActor.QueueStatusUpdate(update) =>
      log.debug("Received new full queue status update")
      currentStats = update.map(i => i.queueName -> i).toMap

    case RabbitQueueStatusActor.PartialStatusUpdate(name, size) =>
      if (naming.isIntegrationQueueName(name)) {
        log.debug(s"Performing incremental update for queue $name to size $size")
        currentStats.get(name) match {
          case Some(info) =>
            currentStats += name -> info.copy(size = size)
          case None =>
            currentStats += name -> SingleQueueDetails(queueName = name, size = size, ready = size, rate = 0.0, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime())
        }
      }

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.stop(statusActor)
      context.become(shutdownImminent, discardOld = false)
  }

  def delegateWorkFromQueues(channel: Channel, worker: ActorRef, queues: List[String]) : Unit = {
    try {
      createExecutionItem(channel, worker, queues) match {
        case None =>
          log.debug("Failed to identify any valid work items for worker")
        case Some(execution) =>
          log.debug(s"Successfully acquired task execution: ${ execution.input.summaryString }")
          worker ! execution
      }
    } catch {
      case e: Exception =>
        log.error(e, "Failed to create work for worker execution")
        throw e
    }
  }

  def createExecutionItem(channel: Channel, worker: ActorRef, queues: Iterator[SingleQueueDetails]): Option[WorkQueueExecution] = {
    var result: Option[WorkQueueExecution] = None
    while (result.isEmpty && queues.hasNext) {
      val queue = queues.next()
      dequeueWithoutAck(channel, queue.queueName) match {
        case None       => log.debug(s"Failed to acquire any work from queue: ${ queue.queueName }")
        case Some(item) =>
          log.debug(s"Found work queue item: ${ item.input.summaryString }")
          resources.leaseResources(channel, item.input.resources) match {
            case Left(leases) =>
              result = Some(WorkQueueExecution(channel, item.headers, item.input, leases))
            case Right(ResourceUnavailable(unavailable)) =>
              log.info(s"Unable to acquire ${ unavailable.inspect } to perform ${ item.input.summaryString }. Sending back to queue.")
              channel.basicReject(item.headers.deliveryTag, true)
          }
      }
    }
    result
  }

  @tailrec
  def createExecutionItem(channel: Channel, worker: ActorRef, queues: List[String]): Option[WorkQueueExecution] = {
    if (queues.isEmpty){
      None
    } else {
      dequeueWithoutAck(channel, queues.head) match {
        case None       => createExecutionItem(channel, worker, queues.tail)
        case Some(item) =>
          log.debug(s"Found work queue item: ${ item.input.summaryString }")
          resources.leaseResources(channel, item.input.resources) match {
            case Left(leases) =>
              Some(WorkQueueExecution(channel, item.headers, item.input, leases))
            case Right(ResourceUnavailable(unavailable)) =>
              log.info(s"Unable to acquire ${ unavailable.inspect } to perform ${ item.input.summaryString }. Sending back to queue.")
              channel.basicReject(item.headers.deliveryTag, true)
              createExecutionItem(channel, worker, queues.tail)
          }
      }
    }
  }

  def integrationQueuePriorities(): Iterator[SingleQueueDetails] = {
    val details = nonEmptyIntegrationQueueGroups().flatMap {
      case single: SingleQueueDetails => Seq(single)
      case multi:  MultiQueueDetails  => Random.shuffle(multi.queues)
    }
    val metrics = details.map(detail => WorkQueueMetrics(null, detail, Seq()))
    prioritizer.prioritize(metrics).map(_.details)
  }

  def integrationQueueOrder: List[SingleQueueDetails] = {
    val groupings = nonEmptyIntegrationQueueGroups()
    if (groupings.isEmpty){
      List()
    } else {
      val timeOrdering = groupings.sortBy(- _.estimatedCompletionTime)
      val longestTime  = timeOrdering.head.estimatedCompletionTime
      val (ties, tail) = timeOrdering.partition(_.estimatedCompletionTime == longestTime)
      val finalOrder   = (ties.sortBy(- _.size) ++ tail).toList
      finalOrder.flatMap {
        case single: SingleQueueDetails => List(single)
        case multi:  MultiQueueDetails  => Random.shuffle(multi.queues).toList
      }
    }
  }

  def nonEmptyIntegrationQueueGroups(): Seq[QueueDetails] = {
    val integrations = currentStats.values.filter(info => {
      naming.isIntegrationQueueName(info.queueName)
    })
    val queueGroups  = naming.toLogicalQueueDetails(integrations)
    queueGroups.filterNot(_.isEmpty).map {
      case single: SingleQueueDetails => single
      case group: MultiQueueDetails   => group.nonEmptyQueues
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
