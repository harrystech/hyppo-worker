package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.google.inject.{Injector, Provider}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.WorkDispatcher
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.sandinh.akuice.ActorInject
import com.typesafe.config.Config

import scala.concurrent.Await


/**
 * Created by jpetty on 8/28/15.
 */
@Singleton
final class HyppoCoordinator @Inject()
(
  injectorProvider: Provider[Injector],
  system:           ActorSystem,
  config:           CoordinatorConfig,
  httpClient:       RabbitHttpClient
) extends WorkDispatcher with ActorInject {

  override def injector: Injector = injectorProvider.get

  private val responseActor   = injectTopActor[ResponseQueueConsumer]("responses")
  private val enqueueProxy    = injectTopActor[EnqueueWorkQueueProxy]("enqueue-proxy")

  //  Attempt a graceful stop
  system.registerOnTermination({
    this.stop()
  })

  def start(): Unit = {
    responseActor ! Lifecycle.ApplicationStarted
  }

  def stop(): Unit = {
    enqueueProxy  ! PoisonPill
    Await.result(gracefulStop(responseActor, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown), config.rabbitMQTimeout)
  }

  override def enqueue(work: WorkerInput) : Unit = {
    enqueueProxy ! work
  }

  override def fetchLogicalHyppoQueueDetails() : Seq[QueueDetails]   = httpClient.fetchLogicalHyppoQueueDetails()

  override def fetchRawHyppoQueueDetails() : Seq[SingleQueueDetails] = httpClient.fetchRawHyppoQueueDetails()
}



object HyppoCoordinator {

  def createConfig(appConfig: Config): CoordinatorConfig = {
    val config = appConfig.withFallback(referenceConfig())

    val merged = requiredConfig().
      withFallback(config)
      .resolve()

    new CoordinatorConfig(merged)
  }

  def requiredConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/required.conf")

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")

}
