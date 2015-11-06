package com.harrys.hyppo.worker.actor.queue

import java.time.ZoneOffset

import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.queue.WorkQueueItem
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation.ResourceUnavailable
import com.harrys.hyppo.worker.actor.{RequestForPreferredWork, RequestForAnyWork, RequestForWork}
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.rabbitmq.client.{ShutdownSignalException, ShutdownListener}
import com.thenewmotion.akka.rabbitmq._

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.util.Random

/**
  * Created by jpetty on 11/6/15.
  */
final class WorkDelegation(config: WorkerConfig, connection: ActorRef) extends Actor with ActorLogging {
  //  Helper objects for dealing with queues
  val serializer    = new AMQPSerialization(context)
  val naming        = new QueueNaming(config)
  val helpers       = new QueueHelpers(config, naming)
  val resources     = new ResourceManagement
  //  The current known information about the queues, updated via assignment once fetched
  var currentStats  = Map[String, QueueStatusInfo]()
  //  Fire off initialization logic
  this.initializeQueuesAsync()

  //  Used internally to signal completion of initial setup
  private case object Initialized


  def shutdownImminent: Receive = {
    case request: RequestForWork =>
      log.debug("Ignoring worker request for work. Shutdown is imminent")
  }

  def initialized: Receive = {
    case Initialized =>
      log.warning("Received duplicate initialization complete messages!")

    case RequestForPreferredWork(prefer) =>
      val worker  = sender()
      val queues  = List(naming.integrationQueueBaseName(prefer), naming.generalQueueName) ++ integrationChoiceOrder.map(_.name)
      delegateWorkFromQueues(worker, queues)

    case RequestForAnyWork =>
      val worker  = sender()
      val queues  = List(naming.generalQueueName) ++ integrationChoiceOrder.map(_.name)
      delegateWorkFromQueues(worker, queues)
  }

  override def receive: Receive = {
    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown is imminent. Ceasing work delegation")
      context.become(shutdownImminent, discardOld = false)
      //  Broadcast to any in-flight zygotes
      context.children.foreach(c => c ! Lifecycle.ImpendingShutdown)

    case Initialized =>
      log.info("Initialization complete. Going to run mode")
      context.become(initialized, discardOld = false)

    case RabbitQueueStatusActor.QueueStatusUpdate(update) =>
      currentStats = update.map(i => i.name -> i).toMap

    case RabbitQueueStatusActor.PartialStatusUpdate(name, size) =>
      if (naming.isIntegrationQueueName(name)) {
        log.debug(s"Performing incremental update for queue $name to size $size")
        currentStats.get(name) match {
          case Some(info) => currentStats += name -> info.copy(size = size)
          case None => currentStats += name -> QueueStatusInfo(name, size, 0.0, TimeUtils.currentLocalDateTime())
        }
      }
  }

  def delegateWorkFromQueues(worker: ActorRef, queues: List[String]) : Unit = {
    //  Creates a channel to handle all work for this request within
    val channelActor = connection.createChannel(ChannelActor.props(setupTaskZygoteChannel))
    channelActor ! ChannelMessage(c => delegateWorkFromQueues(c, channelActor, worker, queues), dropIfNoChannel = false)
  }

  def delegateWorkFromQueues(channel: Channel, channelActor: ActorRef, worker: ActorRef, queues: List[String]) : Unit = {
    try {
      createExecutionItem(channel, channelActor, worker, queues) match {
        case None =>
          log.info("Failed to identify any valid work items for worker")
          context.stop(channelActor)
        case Some(execution) =>
          log.info(s"Successfully created task execution: ${ execution.input.executionId }")
          worker ! execution
      }
    } catch {
      case e: Exception =>
        log.error(e, "Failed to create work for worker execution")
        context.stop(channelActor)
        throw e
    }
  }

  @tailrec
  def createExecutionItem(channel: Channel, channelActor: ActorRef, worker: ActorRef, queues: List[String]) : Option[WorkQueueExecution] = {
    if (queues.isEmpty){
      None
    } else {
      dequeueWithoutAck(channel, queues.head) match {
        case None       => createExecutionItem(channel, channelActor, worker, queues.tail)
        case Some(item) =>
          log.debug(s"Found work queue item: ${ item.input.summaryString }")
          resources.leaseResources(channel, item.input.resources) match {
            case Left(leases) =>
              Some(WorkQueueExecution(channelActor, item.headers, item.input, leases))
            case Right(ResourceUnavailable(unavailable)) =>
              log.info(s"Unable to acquire ${ unavailable.inspect } to perform work. Sending back to queue.")
              channel.basicReject(item.headers.deliveryTag, true)
              createExecutionItem(channel, channelActor, worker, queues.tail)
          }
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
      self ! RabbitQueueStatusActor.PartialStatusUpdate(queueName, 0)
      None
    } else {
      self ! RabbitQueueStatusActor.PartialStatusUpdate(queueName, response.getMessageCount)
      val workerInput = serializer.deserialize[WorkerInput](response.getBody)
      val itemHeaders = new QueueItemHeaders(response.getEnvelope, response.getProps)
      Some(WorkQueueItem(itemHeaders, workerInput))
    }
  }

  /**
    * Creates the channel to be a single-use channel that terminates the channel actor on shutdown
    * @param channel The RabbitMQ channel associated with this zygote
    * @param channelActor The channel actor that will handle the rest of this task zygote's work
    */
  private def setupTaskZygoteChannel(channel: Channel, channelActor: ActorRef) : Unit = {
    channel.addShutdownListener(new ShutdownListener {
      override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
        log.error(s"Channel closed unexpectedly: ${ cause.getMessage }")
        context.stop(channelActor)
      }
    })
  }


  private def initializeQueuesAsync() : Unit = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout * 2)
    val delegator = self
    (connection ? CreateChannel(ChannelActor.props())).mapTo[ChannelCreated].onSuccess {
      case ChannelCreated(channelActor) =>
        channelActor ! ChannelMessage(c => {
          helpers.createExpiredQueue(c)
          helpers.createGeneralWorkQueue(c)
          delegator ! Initialized
          context.stop(channelActor)
        }, dropIfNoChannel = false)
    }
  }
}
