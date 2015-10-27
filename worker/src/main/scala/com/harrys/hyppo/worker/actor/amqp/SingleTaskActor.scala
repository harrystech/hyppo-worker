package com.harrys.hyppo.worker.actor.amqp

import java.util.Date

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.api.proto.{RemoteLogFile, RemoteRawDataFile, FailureResponse, WorkerResponse}
import com.rabbitmq.client.AMQP.BasicProperties

/**
 * Created by jpetty on 9/21/15.
 */
final class SingleTaskActor(config: WorkerConfig, task: WorkQueueItem, commander: ActorRef) extends Actor with ActorLogging {

  //  Establish death-pact with commander
  context.watch(commander)

  val serialization = new AMQPSerialization(context)

  override def preStart() : Unit = {
    super.preStart()
    log.debug(s"Sending work ${ task.input.toString } to commander ${ commander.toString }")
    commander ! task
  }

  override def receive: Receive = {
    case ack @ Ack(deliveryTag) if deliveryTag.equals(task.rabbitItem.deliveryTag) =>
      sendAckForQueueItem(ack)

    case nack @ Reject(deliveryTag, requeue) if deliveryTag.equals(task.rabbitItem.deliveryTag) =>
      sendNackForQueueItem(nack)

    case response: WorkerResponse =>
      publishWorkResponse(response)

    case Terminated(dead) if dead.equals(commander) =>
      log.error(s"Unexpected termination of commander actor ${ commander }")
      val fakeLog = RemoteLogFile(config.dataBucketName, "")
      publishWorkResponse(FailureResponse(task.input, fakeLog, None))
      context.stop(self)
  }

  def sendAckForQueueItem(ack: Ack) : Unit = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)

    (task.rabbitItem.channel ? ack).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully ACK'ed delivery tag: ${ task.rabbitItem.deliveryTag }")
      case Error(_, cause) =>
        log.error(cause, s"Failed to ACK delivery tag: ${ task.rabbitItem.deliveryTag }")
    }
  }

  def sendNackForQueueItem(nack: Reject) : Unit = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)

    (task.rabbitItem.channel ? nack).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully ACK'ed delivery tag: ${ task.rabbitItem.deliveryTag }")
      case Error(_, cause) =>
        log.error(cause, s"Failed to ACK delivery tag: ${ task.rabbitItem.deliveryTag }")
    }
  }

  def publishWorkResponse(response: WorkerResponse) : Unit = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)

    val body  = serialization.serialize(response)
    val props = new BasicProperties.Builder()
      .timestamp(new Date())
      .build()

    log.debug(s"Publishing to queue ${ task.rabbitItem.replyTo } : ${ response.toString }")

    (task.rabbitItem.channel ? Publish("", task.rabbitItem.replyTo, body, Some(props))).collect {
      case Ok(_, _) =>
        log.debug("Successfully published work response to result queue")
      case Error(_, cause) =>
        log.error(cause, "Failed to publish work response to result queue. This may result in duplicated work and inconsistent data!")
    }
  }
}
