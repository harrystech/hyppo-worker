package com.harrys.hyppo.worker.actor.queue

import akka.actor._
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec
import scala.util.Random

/**
  * Created by jpetty on 11/6/15.
  */
final class WorkDelegation(config: WorkerConfig) extends Actor with ActorLogging {
  //  Helper objects for dealing with queues
  val serializer    = new AMQPSerialization(context)
  val naming        = new QueueNaming(config)
  val helpers       = new QueueHelpers(config, naming)
  val resources     = new ResourceLeasing
  //  The current known information about the queues, updated via assignment once fetched
  var currentStats  = Map[String, QueueStatusInfo]()


  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  override def receive: Receive = {

    case RequestForPreferredWork(channelActor, prefer) =>
      val worker  = sender()
      val filter  = naming.belongsToIntegration(prefer) _
      val base    = List(naming.generalQueueName) ++ integrationChoiceOrder.map(_.name)
      //  Moves all queues belong to this integration to the front of the list
      val (head, tail) = base.partition(name => filter(name))
      val queues  = head ++ tail
      channelActor ! ChannelMessage(c => delegateWorkFromQueues(c, worker, queues))

    case RequestForAnyWork(channelActor) =>
      val worker  = sender()
      val queues  = List(naming.generalQueueName) ++ integrationChoiceOrder.map(_.name)
      channelActor ! ChannelMessage(c => delegateWorkFromQueues(c, worker, queues))

    case RabbitQueueStatusActor.QueueStatusUpdate(update) =>
      log.debug("Received new full queue status update")
      currentStats = update.map(i => i.name -> i).toMap

    case RabbitQueueStatusActor.PartialStatusUpdate(name, size) =>
      if (naming.isIntegrationQueueName(name)) {
        log.debug(s"Performing incremental update for queue $name to size $size")
        currentStats.get(name) match {
          case Some(info) =>
            currentStats += name -> info.copy(size = size)
          case None =>
            currentStats += name -> QueueStatusInfo(name, size, 0.0, TimeUtils.currentLocalDateTime())
        }
      }

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.become(shutdownImminent, discardOld = false)
  }

  def delegateWorkFromQueues(channel: Channel, worker: ActorRef, queues: List[String]) : Unit = {
    try {
      createExecutionItem(channel, worker, queues) match {
        case None =>
          log.info("Failed to identify any valid work items for worker")
        case Some(execution) =>
          log.info(s"Successfully created task execution: ${ execution.input.executionId }")
          worker ! execution
      }
    } catch {
      case e: Exception =>
        log.error(e, "Failed to create work for worker execution")
        throw e
    }
  }

  @tailrec
  def createExecutionItem(channel: Channel, worker: ActorRef, queues: List[String]) : Option[WorkQueueExecution] = {
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
              log.info(s"Unable to acquire ${ unavailable.inspect } to perform work. Sending back to queue.")
              channel.basicReject(item.headers.deliveryTag, true)
              createExecutionItem(channel, worker, queues.tail)
          }
      }
    }
  }

  def integrationChoiceOrder: List[QueueStatusInfo] = {
    val ordering = currentStats.values.toIndexedSeq.filterNot(_.isEmpty).sortBy(- _.estimatedCompletionTime)
    if (ordering.isEmpty){
      List()
    } else {
      val longestTime  = ordering.head.estimatedCompletionTime
      val (ties, tail) = ordering.partition(_.estimatedCompletionTime == longestTime)
      (ties.sortBy(- _.size) ++ Random.shuffle(tail)).toList
    }
  }

  def dequeueWithoutAck(channel: Channel, queueName: String) : Option[WorkQueueItem] = {
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
