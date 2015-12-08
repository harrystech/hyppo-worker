package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.{CoordinatorConfig, HyppoConfig}
import com.harrys.hyppo.coordinator.{WorkDispatcher, WorkResponseHandler}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.thenewmotion.akka.rabbitmq._
import com.typesafe.config.Config

import scala.concurrent.Await


/**
 * Created by jpetty on 8/28/15.
 */
@Singleton
final class HyppoCoordinator @Inject() (system: ActorSystem, config: CoordinatorConfig, handler: WorkResponseHandler) extends WorkDispatcher {
  private val rabbitMQApi     = config.newRabbitMQApiClient()
  HyppoCoordinator.initializeBaseQueues(config)

  private val connectionActor = system.actorOf(ConnectionActor.props(config.rabbitMQConnectionFactory, reconnectionDelay = config.rabbitMQTimeout), name = "rabbitmq")
  private val responseActor   = system.actorOf(Props(classOf[ResponseQueueConsumer], config, connectionActor, handler), name = "responses")
  private val enqueueProxy    = system.actorOf(Props(classOf[EnqueueWorkQueueProxy], config), name = "enqueue-proxy")

  system.registerOnTermination(new Runnable {
    override def run(): Unit = {
      Await.result(gracefulStop(responseActor, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown), config.rabbitMQTimeout)
    }
  })

  override def enqueue(work: WorkerInput) : Unit = {
    enqueueProxy ! work
  }

  override def fetchLogicalHyppoQueueDetails() : Seq[QueueDetails]   = rabbitMQApi.fetchLogicalHyppoQueueDetails()
  override def fetchRawHyppoQueueDetails() : Seq[SingleQueueDetails] = rabbitMQApi.fetchRawHyppoQueueDetails()
}



object HyppoCoordinator {

  def apply(system: ActorSystem, config: CoordinatorConfig, handler: WorkResponseHandler) : HyppoCoordinator  = {
    new HyppoCoordinator(system, config, handler)
  }

  def apply(system: ActorSystem, dispatcher: WorkDispatcher, handler: WorkResponseHandler) : HyppoCoordinator = {
    val config = createConfig(system.settings.config)
    apply(system, config, handler)
  }

  def createConfig(appConfig: Config) : CoordinatorConfig = {
    val config = appConfig.withFallback(referenceConfig())

    val merged = requiredConfig().
      withFallback(config)
      .resolve()

    new CoordinatorConfig(merged)
  }

  def requiredConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/required.conf")

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")

  def initializeBaseQueues(config: HyppoConfig): Unit = {
    val helpers    = new QueueHelpers(config)
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      val channel  = connection.createChannel()
      helpers.createExpiredQueue(channel)
      helpers.createGeneralWorkQueue(channel)
      helpers.createResultsQueue(channel)
      channel.close()
    } finally {
      connection.close()
    }
  }
}
