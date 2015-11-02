package com.harrys.hyppo.worker.actor.amqp

import akka.actor.PoisonPill
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.harrys.hyppo.Lifecycle.ImpendingShutdown
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.WorkResponseHandler
import com.harrys.hyppo.worker.api.proto._

import scala.concurrent.Await
import scala.util.Success

/**
 * Created by jpetty on 9/16/15.
 */
final class RabbitResponseQueueConsumer(config: CoordinatorConfig, handler: WorkResponseHandler) extends RabbitParticipant {
  //  Bring the dispatcher into scope
  import context.dispatcher

  //  Establish the RabbitMQ connection and channel to use for work tasks
  val connection = createRabbitConnection(config)
  val consumer   = Await.result(HyppoQueue.createResultsQueueConsumer(context, config, connection), config.rabbitMQTimeout)

  override def receive: Receive = {
    case delivery: Delivery =>
      try {
        val response = deserialize[WorkerResponse](delivery.body)
        log.debug(s"Received delivery ${delivery.envelope.getDeliveryTag} with body ${response.toString}")
        handleSingle(response)
        sendAck(delivery)
      } catch {
        case e: Exception =>
          sendReject(e, delivery)
          throw e
      }
    case ImpendingShutdown =>
      implicit val timeout = Timeout(config.rabbitMQTimeout)
      (consumer.consumer ? CancelConsumer(consumer.consumerTag)).onComplete({
        case Success(Ok(_, _)) =>
          log.info(s"Consumer ${consumer.consumerTag} successfully cancelled")
          self ! PoisonPill
        case other =>
          log.error(s"Failed to cancel consumer: ${consumer.consumerTag}")
      })
  }


  def handleSingle(unmatched: WorkerResponse) : Unit = unmatched match {
    case r: ValidateIntegrationResponse  => handler.onIntegrationValidated(r)
    case r: CreateIngestionTasksResponse => handler.onIngestionTasksCreated(r)
    case r: FetchRawDataResponse         => handler.onRawDataFetched(r)
    case r: ProcessRawDataResponse       => handler.onRawDataProcessed(r)
    case r: FetchProcessedDataResponse   => handler.onProcessedDataFetched(r)
    case r: PersistProcessedDataResponse => handler.onProcessedDataPersisted(r)
    case r: FailureResponse              => handler.onWorkFailed(r)
  }

  def sendAck(delivery: Delivery) : Unit = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    log.debug(s"Sending ACK for delivery: ${delivery.envelope.getDeliveryTag}")
    (consumer.consumer ? Ack(delivery.envelope.getDeliveryTag)).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully ACK'ed delivery ${delivery.envelope.getDeliveryTag}")
      case Error(_, cause) =>
        log.error(cause, s"Failed to send ACK for delivery ${delivery.envelope.getDeliveryTag}")
    }
  }

  def sendReject(e: Exception, delivery: Delivery) : Unit = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    log.error(e, "Failure inside work response handling")
    (consumer.consumer ? Reject(delivery.envelope.getDeliveryTag)).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully rejected delivery ${delivery.envelope.getDeliveryTag}")
      case Error(_, cause) =>
        log.error(cause, s"Failed to send rejection notice for delivery ${delivery.envelope.getDeliveryTag}")
    }
  }
}
