package com.harrys.hyppo.worker.actor.amqp

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, ConnectionOwner, Consumer}
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.rabbitmq.client.AMQP

import scala.concurrent.Future

/**
 * Created by jpetty on 9/14/15.
 */
object HyppoQueue {

  final val WorkQueuePrefix  = "hyppo.work.queue"

  final val ResultsQueueName = "hyppo.results"

  final val GeneralWorkQueue = s"$WorkQueuePrefix.general"

  final val IntegrationQueuePrefix = s"$WorkQueuePrefix.integration"

  def integrationQueueName(integration: ExecutableIntegration) : String = {
    val sourceFix = integration.sourceName.replaceAll("\\s", "_")
    val version   = s"version-${integration.details.versionNumber}"
    s"$IntegrationQueuePrefix.$sourceFix.$version"
  }

  def generalWorkQueueParams() : QueueParameters = {
    QueueParameters(GeneralWorkQueue, passive = false, durable = true, exclusive = false, autodelete = false)
  }

  def integrationWorkQueueParams(integration: ExecutableIntegration) : QueueParameters = {
    val name = integrationQueueName(integration)
    QueueParameters(name, passive = false, durable = true, exclusive = false, autodelete = false)
  }

  def resultsQueueParams() : QueueParameters = {
    QueueParameters(ResultsQueueName, passive = false, durable = true, exclusive = false, autodelete = false)
  }

  def createIntegrationQueue(integration: ExecutableIntegration, context: ActorContext, config: HyppoConfig, channel: ActorRef) : Future[QueueParameters] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, channel).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)

    val log    = Logging(context.system, context.self)
    val params = integrationWorkQueueParams(integration)

    (channel ? DeclareQueue(params)).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully created queue: ${params.name}")
        params
      case Error(_, cause) =>
        log.error(cause, s"Failed to create queue: ${params.name}")
        throw cause
    }
  }

  def createResultsQueueConsumer(context: ActorContext, config: HyppoConfig, connection: ActorRef) : Future[QueueConsumer] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, connection).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)
    val consumer = context.watch(ConnectionOwner.createChildActor(connection, Consumer.props(context.self, channelParams = Some(ChannelParameters(qos = 1)), autoack = false), name = Some("results-consumer")))

    val log   = Logging(context.system, context.self)
    val queue = createResultsQueue(context, config, consumer)

    queue.flatMap { params =>
      (consumer ? AddQueue(params)).collect {
        case Ok(_, Some(consumerTag: String)) =>
          log.debug(s"Successfully bound queue: ${params.name} to consumer with tag: $consumerTag")
          QueueConsumer(params, consumerTag, consumer)
        case Error(_, cause) =>
          log.error(cause, s"Failure adding queue ${params.name} to consumer: ${consumer.toString()}")
          throw cause
      }
    }
  }

  def createResultsQueue(context: ActorContext, config: HyppoConfig, channel: ActorRef) : Future[QueueParameters] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, channel).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)

    val log    = Logging(context.system, context.self)
    val params = resultsQueueParams()

    (channel ? DeclareQueue(params)).collect {
      case Ok(request, Some(result: AMQP.Queue.DeclareOk))  =>
        log.debug(s"Successfully created queue: ${result.getQueue}")
        params
      case Error(request, cause) =>
        log.error(cause, s"Failed to create queue: ${params.name}")
        throw cause
    }
  }

  def createGeneralWorkQueue(context: ActorContext, config: HyppoConfig, channel: ActorRef) : Future[QueueParameters] = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    Amqp.waitForConnection(context.system, channel).await(config.rabbitMQTimeout.toMillis, TimeUnit.MILLISECONDS)

    val log    = Logging(context.system, context.self)
    val params = generalWorkQueueParams()

    (channel ? DeclareQueue(params)).collect {
      case Ok(request, Some(result: AMQP.Queue.DeclareOk))  =>
        log.debug(s"Successfully created queue: ${result.getQueue}")
        params
      case Error(request, cause) =>
        log.error(cause, s"Failed to create queue: ${params.name}")
        throw cause
    }
  }
}
