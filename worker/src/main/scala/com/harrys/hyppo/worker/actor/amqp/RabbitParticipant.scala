package com.harrys.hyppo.worker.actor.amqp

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, ConnectionOwner, Consumer}
import com.harrys.hyppo.config.HyppoConfig
import com.rabbitmq.client.AMQP

import scala.concurrent.Future
import scala.reflect._

/**
 * Created by jpetty on 9/5/15.
 */
trait RabbitParticipant extends Actor with ActorLogging {

  //  Made lazy here to address initialization order issues
  private lazy val serialization = new AMQPSerialization(context)

  final def serialize(o: AnyRef) : Array[Byte] = serialization.serialize(o)

  final def deserialize[T : ClassTag](bytes: Array[Byte]) : T = serialization.deserialize[T](bytes)

  def createRabbitConnection(config: HyppoConfig) : ActorRef = {
    val amqp = config.rabbitMQConnectionFactory
    log.info(s"Connecting to RabbitMQ - amqp://${amqp.getUsername}:${amqp.getPassword}@${amqp.getHost}:${amqp.getPort()}/${amqp.getVirtualHost}")
    context.watch(context.actorOf(ConnectionOwner.props(config.rabbitMQConnectionFactory, config.rabbitMQTimeout), name = "rabbitmq"))
  }

  def createExclusiveQueue(config: HyppoConfig, connection: ActorRef, channel: ActorRef) : Future[QueueParameters] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, connection, channel).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)
    val params  = QueueParameters(name = "", passive = false, durable = false, exclusive = true, autodelete = false)

    (channel ? DeclareQueue(params)).collect {
      case Ok(request, Some(result: AMQP.Queue.DeclareOk))  =>
        log.debug(s"Successfully bound new queue: ${result.getQueue}")
        params.copy(name = result.getQueue, passive = true)
      case Error(request, cause) =>
        log.error(cause, s"Failed to bind exclusive queue")
        throw cause
    }
  }

  def createExclusiveQueueConsumer(config: HyppoConfig, connection: ActorRef, name: String) : Future[QueueConsumer] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, connection).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)
    val consumer    = context.watch(ConnectionOwner.createChildActor(connection, Consumer.props(self, channelParams = Some(ChannelParameters(qos = 1)), autoack = true), name = Some(name)))
    val queueFuture = createExclusiveQueue(config, connection, consumer)
    Amqp.waitForConnection(context.system, connection, consumer).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)

    queueFuture.flatMap { params =>
      (consumer ? AddQueue(params)).collect {
        case Ok(_, Some(result: String)) =>
          log.debug(s"Successfully bound queue: ${params.name} to consumer with tag: $result")
          QueueConsumer(params, result, consumer)
        case Error(_, cause) =>
          log.error(cause, s"Failure adding queue ${params.name} to consumer: ${consumer.toString()}")
          throw cause
      }
    }
  }
}
