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
  val serializer          = new AMQPSerialization(context)
  val responseConsumer    = connection.createChannel(ChannelActor.props(configureResponseQueueConsumer), name = Some("consumer-channel"))
  var consumerTag: String = null

  private final case class ConsumerRegistration(consumerTag: String)

  override def receive: Receive = {
    case ConsumerRegistration(tag) =>
      log.info(s"Successfully registered consumer: $tag")
      consumerTag = tag

    case ImpendingShutdown =>
      log.info("Shutting down consumer")
      context.stop(self)
      if (consumerTag != null){
        val tag = consumerTag
        responseConsumer ! ChannelMessage(c => c.basicCancel(tag))
        Await.ready(gracefulStop(responseConsumer, config.rabbitMQTimeout), config.rabbitMQTimeout)
      }
  }

  def configureResponseQueueConsumer(channel: Channel, channelActor: ActorRef) : Unit = {
    channel.basicQos(1)
    val queueName   = queueHelpers.createResultsQueue(channel).getQueue
    val autoAck     = false
    val consumerTag = channel.basicConsume(queueName, autoAck, new ResponseConsumer(channel))
    log.debug(s"Starting consumer $consumerTag to listen on queue: $queueName")
    self ! ConsumerRegistration(consumerTag)
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
          log.error(s"Failed to process delivery ${ envelope.getDeliveryTag }", e)
          channel.basicReject(envelope.getDeliveryTag, true)
          throw e
      }
    }

    private def handleSingle(unmatched: WorkerResponse) : Unit = unmatched match {
      case r: ValidateIntegrationResponse  => handler.onIntegrationValidated(r)
      case r: CreateIngestionTasksResponse => handler.onIngestionTasksCreated(r)
      case r: FetchRawDataResponse         => handler.onRawDataFetched(r)
      case r: ProcessRawDataResponse       => handler.onRawDataProcessed(r)
      case r: FetchProcessedDataResponse   => handler.onProcessedDataFetched(r)
      case r: PersistProcessedDataResponse => handler.onProcessedDataPersisted(r)
      case r: FailureResponse              => handler.onWorkFailed(r)
    }
  }
}
