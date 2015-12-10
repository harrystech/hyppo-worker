package com.harrys.hyppo.worker.actor.amqp

import akka.actor.{Actor, ActorLogging}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.api.proto._
import com.thenewmotion.akka.rabbitmq._

import scala.util.{Failure, Success, Try}

/**
 * Created by jpetty on 9/16/15.
 */
final class EnqueueWorkQueueProxy(config: CoordinatorConfig) extends Actor with ActorLogging {

  val serializer      = new AMQPSerialization(config.secretKey)
  val queueNaming     = new QueueNaming(config)
  val queueHelpers    = new QueueHelpers(config, queueNaming)
  val localConnection = config.rabbitMQConnectionFactory.newConnection()
  val enqueueChannel  = localConnection.createChannel()
  initializeChannel(enqueueChannel)

  override def postStop() : Unit = {
    Try(enqueueChannel.close())
    Try(localConnection.close())
  }

  private final case class RetryEnqueue(work: WorkerInput)

  override def receive: Receive = {
    case work: WorkerInput =>
      Try(publishToQueue(work)) match {
        case Success(_) => log.debug(s"Successfully enqueued work execution: ${ work.executionId }")
        case Failure(e) =>
          log.error(e, s"Failed to enqueue work execution: ${ work.executionId }. Retrying after actor restart")
          self ! RetryEnqueue(work)
          throw e
      }

    case RetryEnqueue(work) =>
      Try(publishToQueue(work)) match {
        case Success(_) => log.debug(s"Successfully enqueued work execution after retry: ${ work.executionId }")
        case Failure(e) =>
          log.error(e, s"Permanently failed to enqueue work execution: ${ work.executionId }")
          throw e
      }
  }

  def publishToQueue(work: WorkerInput): Unit = {
    val queueName = work match {
      case generalWork: GeneralWorkerInput      => queueNaming.generalQueueName
      case specificWork: IntegrationWorkerInput => queueHelpers.createIntegrationQueue(enqueueChannel, specificWork).getQueue
    }
    createRequiredResources(work)
    val body  = serializer.serialize(work)
    val props = AMQPMessageProperties.enqueueProperties(work.executionId, queueNaming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
    enqueueChannel.basicPublish("", queueName, true, false, props, body)
    enqueueChannel.waitForConfirms(config.rabbitMQTimeout.toMillis)
  }

  def createRequiredResources(work: WorkerInput) : Unit = {
    work.resources.foreach {
      case c: ConcurrencyWorkResource =>
        queueHelpers.createConcurrencyResource(localConnection, c)
      case t: ThrottledWorkResource =>
        queueHelpers.createThrottledResource(localConnection, t)
    }
  }


  def initializeChannel(channel: Channel): Unit = {
    queueHelpers.createExpiredQueue(channel)
    queueHelpers.createResultsQueue(channel)
    queueHelpers.createGeneralWorkQueue(channel)
    channel.confirmSelect()
  }
}
