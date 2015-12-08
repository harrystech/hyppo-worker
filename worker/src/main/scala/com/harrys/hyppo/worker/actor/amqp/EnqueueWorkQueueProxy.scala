package com.harrys.hyppo.worker.actor.amqp

import akka.actor.{Actor, ActorLogging}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.api.proto._
import com.thenewmotion.akka.rabbitmq._

import scala.util.Try

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

  override def receive: Receive = {
    case work: GeneralWorkerInput     =>
      publishToQueue(queueNaming.generalQueueName, work)

    case work: IntegrationWorkerInput =>
      val queue = queueHelpers.createIntegrationQueue(enqueueChannel, work).getQueue
      publishToQueue(queue, work)
  }

  def publishToQueue(queue: String, work: WorkerInput): Unit = {
    createRequiredResources(work)
    val body  = serializer.serialize(work)
    val props = AMQPMessageProperties.enqueueProperties(work.executionId, queueNaming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
    enqueueChannel.basicPublish("", queue, true, false, props, body)
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
