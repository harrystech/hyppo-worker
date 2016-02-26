package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.google.inject.{Provider, Injector}
import com.harrys.hyppo.config.{CoordinatorConfig, HyppoConfig}
import com.harrys.hyppo.coordinator.{WorkDispatcher, WorkResponseHandler}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq._
import com.typesafe.config.Config

import scala.concurrent.Await


/**
 * Created by jpetty on 8/28/15.
 */
@Singleton
final class HyppoCoordinator @Inject()
(
  system:     ActorSystem,
  config:     CoordinatorConfig,
  httpClient: RabbitHttpClient,
  injectorProvider: Provider[Injector]
) extends WorkDispatcher with ActorInject {

  override def injector: Injector = injectorProvider.get

  HyppoCoordinator.initializeBaseQueues(config)

  private val responseActor   = injectTopActor[ResponseQueueConsumer]("responses")
  private val enqueueProxy    = injectTopActor[EnqueueWorkQueueProxy]("enqueue-proxy")

  system.registerOnTermination(new Runnable {
    override def run(): Unit = stop()
  })

  def start(): Unit = {
    responseActor ! Lifecycle.ApplicationStarted
  }

  def stop(): Unit = {
    enqueueProxy ! PoisonPill
    Await.result(gracefulStop(responseActor, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown), config.rabbitMQTimeout)
  }

  override def enqueue(work: WorkerInput) : Unit = {
    enqueueProxy ! work
  }

  override def fetchLogicalHyppoQueueDetails() : Seq[QueueDetails]   = httpClient.fetchLogicalHyppoQueueDetails()
  override def fetchRawHyppoQueueDetails() : Seq[SingleQueueDetails] = httpClient.fetchRawHyppoQueueDetails()
}



object HyppoCoordinator {

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
