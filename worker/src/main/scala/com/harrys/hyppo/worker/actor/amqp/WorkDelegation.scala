package com.harrys.hyppo.worker.actor.amqp

import java.time.ZoneOffset

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork, RequestForWork}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec
import scala.util.Random

/**
  * Created by jpetty on 11/4/15.
  */
final class WorkDelegation(config: WorkerConfig, connection: ActorRef) extends Actor with ActorLogging {

  val serializer    = new AMQPSerialization(context)
  val naming        = new QueueNaming(config)
  val helpers       = new QueueHelpers(config, naming)
  val channelActor  = context.watch(connection.createChannel(ChannelActor.props(setupDelegationChannel), name = Some("dequeue-channel")))

  //  The current known information about the queues, updated via assignment once fetched
  var currentStats  = Map[String, QueueStatusInfo]()

  //  Used internally to signal back more recent status updates
  private final case class IncrementalQueueUpdate(name: String, size: Int)


  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }


  override def receive: Receive = {
    case RabbitQueueStatusActor.QueueStatusUpdate(update) =>
      currentStats = update.map(i => i.name -> i).toMap

    case IncrementalQueueUpdate(name, size) =>
      if (naming.isIntegrationQueueName(name)){
        log.debug(s"Performing incremental update for queue $name to size $size")
        currentStats.get(name) match {
          case Some(info) => currentStats += name -> info.copy(size = size)
          case None       => currentStats += name -> QueueStatusInfo(name, size, 0.0, TimeUtils.currentLocalDateTime())
        }
      }

    case request: RequestForWork =>
      val worker = sender()
      log.debug(s"Received ${request.toString} from worker $worker")
      request match {
        case RequestForPreferredWork(prefer) =>
          channelActor ! ChannelMessage(c => nextItemOfPreferredWork(c, worker, prefer))
        case RequestForAnyWork =>
          channelActor ! ChannelMessage(c => nextItemOfAnyWork(c, worker))
      }

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.unwatch(channelActor)
      channelActor ! PoisonPill
      context.become(shutdownImminent)
  }

  def nextItemOfAnyWork(channel: Channel, worker: ActorRef) : Unit = {
    dequeueWithoutAck(channel, naming.generalQueueName) match {
      case Some(work) =>
        worker ! work
      case None =>
        firstItemFromIntegrationList(channel, worker, integrationChoiceOrder)
    }
  }

  def nextItemOfPreferredWork(channel: Channel, worker: ActorRef, integration: ExecutableIntegration) : Unit = {
    dequeueWithoutAck(channel, naming.integrationQueueName(integration)) match {
      case Some(work) =>
        worker ! work
      case None =>
        nextItemOfAnyWork(channel, worker)
    }
  }

  @tailrec
  def firstItemFromIntegrationList(channel: Channel, worker: ActorRef, queues: List[QueueStatusInfo]) : Unit = {
    if (queues.isEmpty){
      log.debug("No work available from any integration queue. Sending no response to worker")
    } else {
      dequeueWithoutAck(channel, queues.head.name) match {
        case Some(work) =>
          worker ! work
        case None  =>
          firstItemFromIntegrationList(channel, worker, queues.tail)
      }
    }
  }


  def integrationChoiceOrder: List[QueueStatusInfo] = {
    val ordering = currentStats.values.toIndexedSeq.filterNot(_.isEmpty).sortBy(- _.estimatedCompletionTime)
    if (ordering.isEmpty){
      List()
    } else {
      var ties = ordering.filter(_.estimatedCompletionTime.equals(ordering.head.estimatedCompletionTime))
      ties = ties.sortBy(_.idleSince.toInstant(ZoneOffset.UTC).toEpochMilli)
      ties = ties.filter(_.idleSince.isEqual(ties.head.idleSince))
      Random.shuffle(ties).toList
    }
  }

  def dequeueWithoutAck(channel: Channel, queueName: String) : Option[WorkQueueItem] = {
    val response = channel.basicGet(queueName, false)
    if (response == null){
      sendIncrementalUpdate(queueName, 0)
      None
    } else {
      sendIncrementalUpdate(queueName, response.getMessageCount)
      val rabbitItem  = RabbitQueueItem(channelActor, response)
      val workerInput = serializer.deserialize[WorkerInput](response.getBody)
      Some(WorkQueueItem(rabbitItem, workerInput))
    }
  }

  def sendIncrementalUpdate(queueName: String, size: Int) : Unit = {
    if (naming.isIntegrationQueueName(queueName)){
      self ! IncrementalQueueUpdate(queueName, size)
    }
  }

  def setupDelegationChannel(channel: Channel, self: ActorRef) : Unit = {
    helpers.createExpiredQueue(channel)
    helpers.createGeneralWorkQueue(channel)
  }
}
