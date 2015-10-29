package com.harrys.hyppo.worker.actor.amqp

import java.time.ZoneOffset

import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{ChannelOwner, ConnectionOwner}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.rabbitmq.client.GetResponse

import scala.concurrent.{Await, Future}
import scala.util.Random

/**
 * Created by jpetty on 9/16/15.
 */
final class RabbitWorkerDelegation(config: WorkerConfig) extends RabbitParticipant {
  //  Bring the execution context into scope
  import context.dispatcher

  //  Establish the RabbitMQ connection and channel to use for work tasks
  val connection = createRabbitConnection(config)
  val channel    = context.watch(ConnectionOwner.createChildActor(connection, ChannelOwner.props(), name = Some("delegation-channel")))

  val resultsQueue = Await.result(HyppoQueue.createResultsQueue(context, config, channel), config.rabbitMQTimeout)
  val generalQueue = Await.result(HyppoQueue.createGeneralWorkQueue(context, config, channel), config.rabbitMQTimeout)

  //  The current known information about the queues, updated via assignment once fetched
  var currentStats = Seq[QueueStatusInfo]()


  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  override def receive: Receive = {
    case RabbitQueueStatusActor.QueueStatusUpdate(update) =>
      currentStats = update

    case request: RequestForWork =>
      val worker = sender()
      log.debug(s"Received ${request.toString} from worker $worker")
      val workFuture = request match {
        case RequestForAnyWork =>
          nextItemOfAnyWork()
        case RequestForPreferredWork(prefer) =>
          nextItemOfPreferredWork(prefer)
      }
      workFuture.collect({
        case None =>
          log.debug("Found no available work to be performed")
        case Some(item) =>
          log.debug(s"Sending ${item.input.toString} to worker $worker")
          worker ! item
      })

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.become(shutdownImminent)
  }

  def integrationChoiceOrder: List[QueueStatusInfo] = {
    val ordering = currentStats.filterNot(_.isEmpty).sortBy(- _.estimatedCompletionTime).toIndexedSeq
    if (ordering.isEmpty){
      List()
    } else {
      var ties = ordering.filter(_.estimatedCompletionTime.equals(ordering.head.estimatedCompletionTime))
      ties = ties.sortBy(_.idleSince.toInstant(ZoneOffset.UTC).toEpochMilli)
      ties = ties.filter(_.idleSince.isEqual(ties.head.idleSince))
      Random.shuffle(ties).toList
    }
  }

  def nextItemOfAnyWork() : Future[Option[WorkQueueItem]] = {
    nextItemOfGeneralWork().flatMap({
      case Some(work) =>
        Future.successful(Some(work))
      case None =>
        nextItemOfAnyIntegrationWork()
    })
  }

  def nextItemOfPreferredWork(prefer: ExecutableIntegration) : Future[Option[WorkQueueItem]] = {
    dequeueWithoutAck(HyppoQueue.integrationQueueName(prefer)).flatMap({
      case Some(work) => Future.successful(Some(work))
      case None => nextItemOfAnyWork()
    })
  }

  def nextItemOfGeneralWork() : Future[Option[WorkQueueItem]] = {
    dequeueWithoutAck(generalQueue.name)
  }

  def nextItemOfAnyIntegrationWork() : Future[Option[WorkQueueItem]] = {
    firstWorkItemInQueueList(integrationChoiceOrder)
  }

  def firstWorkItemInQueueList(ordering: List[QueueStatusInfo]) : Future[Option[WorkQueueItem]] = {
    if (ordering.isEmpty){
      Future.successful(None)
    } else {
      dequeueWithoutAck(ordering.head.name).flatMap({
        case item @ Some(_) => Future.successful(item)
        case None           => firstWorkItemInQueueList(ordering.tail)
      })
    }
  }

  def dequeueWithoutAck(name: String) : Future[Option[WorkQueueItem]] = {
    log.debug(s"Attempting dequeue from queue: $name")

    implicit val timeout = Timeout(config.rabbitMQTimeout)
    val reply  = channel ? Get(name, autoAck = false)

    val result = reply.map({
      case Ok(_, Some(value)) =>
        if (value == null) {
          log.debug(s"No work available in queue: $name")
          None
        } else {
          val delivery = value.asInstanceOf[GetResponse]
          val rabbit   = RabbitQueueItem(channel, delivery)
          log.debug(s"Successfully received work from queue: $name")
          val input = deserialize[WorkerInput](delivery.getBody)
          Some(WorkQueueItem(rabbit, input))
        }
      case Error(request, cause) =>
        log.error(s"Failed to dequeue from queue: $name", cause)
        throw cause
    })

    result.onFailure {
      case error => log.error(error, s"Failed to retrieve work from queue: $name")
    }

    result
  }
}
