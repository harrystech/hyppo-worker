package com.harrys.hyppo.worker.actor.amqp

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.gracefulStop
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.WorkResponseHandler
import com.harrys.hyppo.worker.api.proto._
import com.rabbitmq.client.Channel
import com.thenewmotion.akka.rabbitmq._

import scala.concurrent.Await

/**
 * Created by jpetty on 9/16/15.
 */
final class ResponseQueueConsumer(config: CoordinatorConfig, connection: ActorRef, handler: WorkResponseHandler) extends Actor with ActorLogging {
  val queueHelpers        = new QueueHelpers(config)
  val serializer          = new AMQPSerialization(config.secretKey)
  val consumerChannel     = connection.createChannel(ChannelActor.props(configureResponseQueueConsumer), name = Some("consumer-channel"))

  override def receive: Receive = {
    case ImpendingShutdown =>
      log.info("Shutting down consumer")
      Await.ready(gracefulStop(consumerChannel, config.rabbitMQTimeout), config.rabbitMQTimeout)
      context.stop(self)
  }

  def configureResponseQueueConsumer(channel: Channel, channelActor: ActorRef) : Unit = {
    channel.basicQos(1)
    val autoAck     = false
    val resultQueue = queueHelpers.createResultsQueue(channel).getQueue
    val expireQueue = queueHelpers.createExpiredQueue(channel).getQueue
    val resultTag   = channel.basicConsume(resultQueue, autoAck, new ResponseConsumer(channel))
    log.debug(s"Starting response consumer $resultTag to listen on queue: $resultQueue")
    val expireTag   = channel.basicConsume(expireQueue, autoAck, new ExpiredConsumer(channel))
    log.debug(s"Starting expired consumer $expireTag to consumer from queue: $expireQueue")
  }

  private final class ResponseConsumer(channel: Channel) extends DefaultConsumer(channel) {

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) : Unit = {
      try {
        val response = serializer.deserialize[WorkerResponse](body)
        log.debug(s"Received delivery ${envelope.getDeliveryTag} with body ${response.toString}")
        handleSingle(response)
        channel.basicAck(envelope.getDeliveryTag, false)
      } catch {
        case e: Exception =>
          log.error(s"Failed to process response delivery ${ envelope.getDeliveryTag }", e)
          channel.basicReject(envelope.getDeliveryTag, true)
          throw e
      }
    }

    private def handleSingle(response: WorkerResponse) : Unit = response match {
      case failed: FailureResponse => handler.handleWorkFailed(failed)
      case normal => handler.handleWorkCompleted(normal)
    }
  }

  private final class ExpiredConsumer(channel: Channel) extends DefaultConsumer(channel) {
    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) : Unit = {
      try {
        val expired = serializer.deserialize[WorkerInput](body)
        log.debug(s"Received expired work delivery ${envelope.getDeliveryTag} with body ${expired.toString}")
        handler.handleWorkExpired(expired)
        channel.basicAck(envelope.getDeliveryTag, false)
      } catch {
        case e: Exception =>
          log.error(s"Failed to process delivery ${ envelope.getDeliveryTag }", e)
          channel.basicReject(envelope.getDeliveryTag, true)
          throw e
      }
    }
  }
}
