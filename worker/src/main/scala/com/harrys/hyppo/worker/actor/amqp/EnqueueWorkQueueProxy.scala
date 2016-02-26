package com.harrys.hyppo.worker.actor.amqp

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.api.proto._

import scala.util.{Failure, Success, Try}

/**
 * Created by jpetty on 9/16/15.
 */
final class EnqueueWorkQueueProxy @Inject()
(
  config:       CoordinatorConfig,
  queueNaming:  QueueNaming,
  queueHelpers: QueueHelpers
) extends Actor with ActorLogging {

  val serializer      = new AMQPSerialization(config.secretKey)
  val localConnection = config.rabbitMQConnectionFactory.newConnection()
  queueHelpers.initializeRequiredQueues(localConnection)
  val enqueueChannel  = localConnection.createChannel()
  enqueueChannel.confirmSelect()

  override def postStop() : Unit = {
    Try(enqueueChannel.close())
    Try(localConnection.close())
  }

  private final case class RetryEnqueue(work: WorkerInput)

  override def receive: Receive = {
    case work: WorkerInput =>
      Try(publishToQueue(work)) match {
        case Success(_) =>
          log.debug("Successfully enqueued work execution: {}", work.executionId)
        case Failure(e) =>
          log.error(e, "Failed to enqueue work execution: {}. Retrying after actor restart", work.executionId)
          self ! RetryEnqueue(work)
          throw new Exception("Restarting enqueue proxy due to enqueue failure", e)
      }

    case RetryEnqueue(work) =>
      Try(publishToQueue(work)) match {
        case Success(_) =>
          log.debug("Successfully enqueued work execution after retry: {}", work.executionId)
        case Failure(e) =>
          log.error(e, "Permanently failed to enqueue work execution: {}", work.executionId)
          throw new Exception("Permanent failure enqueueing work", e)
      }
  }

  def publishToQueue(work: WorkerInput): Unit = {
    val queueName = work match {
      case _: GeneralWorkerInput     => queueNaming.generalQueueName
      case i: IntegrationWorkerInput => queueHelpers.createIntegrationQueue(enqueueChannel, i).getQueue
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
}
