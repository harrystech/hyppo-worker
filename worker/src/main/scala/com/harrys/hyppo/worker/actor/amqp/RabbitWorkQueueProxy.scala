package com.harrys.hyppo.worker.actor.amqp

import java.util.UUID

import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{ChannelOwner, ConnectionOwner}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.worker.api.proto.{GeneralWorkerInput, IntegrationWorkerInput, WorkerInput}

import scala.concurrent.Await

/**
 * Created by jpetty on 9/16/15.
 */
final class RabbitWorkQueueProxy(config: CoordinatorConfig) extends RabbitParticipant {
  //  Bring the dispatcher into scope
  import context.dispatcher

  //  Establish the RabbitMQ connection and channel to use for work tasks
  val connection = createRabbitConnection(config)
  val channel    = context.watch(ConnectionOwner.createChildActor(connection, ChannelOwner.props(), name = Some("enqueue-channel")))
  val generalQueue = Await.result(HyppoQueue.createGeneralWorkQueue(context, config, channel), config.rabbitMQTimeout)

  override def receive: Receive = {
    case work: GeneralWorkerInput     => publishGeneralWork(work)
    case work: IntegrationWorkerInput => publishIntegrationWork(work)
  }

  def publishGeneralWork(work: GeneralWorkerInput) : Unit = {
    publish(generalQueue.name, work)
  }


  def publishIntegrationWork(work: IntegrationWorkerInput) : Unit = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)

    //  TODO: Consider optimizing this by not re-declaring the queue for each item enqueued. Might
    // require keeping an set of known queue names and checking it.
    HyppoQueue.createIntegrationQueue(work.integration, context, config, channel).map { params =>
      publish(params.name, work)
    }
  }

  private def publish(queue: String, work: WorkerInput) : Unit = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)

    val body  = serialize(work)
    val props = AMQPMessageProperties.enqueueProperties(UUID.randomUUID(), HyppoQueue.ResultsQueueName, config.workTimeout)

    (channel ? Publish("", queue, body, Some(props))).collect {
      case Ok(_, _) =>
        log.debug(s"Successfully publish work to queue: $queue")
      case Error(_, e) =>
        log.error(e, s"Failed to publish work to queue: $queue")
    }
  }

  def removeOldQueues(queues: Seq[String]) : Unit = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    queues.foreach { name =>
      log.debug(s"Removing idle unused integration queue: $name")
      (channel ? DeleteQueue(name, ifEmpty = true, ifUnused = true)).collect {
        case Ok(_, _) =>
          log.info(s"Successfully deleted old unused queue: $name")
        case Error(_, cause) =>
          log.error(cause, s"Failed to delete old unused queue: $name")
      }
    }
  }
}
