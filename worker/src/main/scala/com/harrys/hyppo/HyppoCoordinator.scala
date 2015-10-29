package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.{WorkDispatcher, WorkResponseHandler}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.amqp.{QueueStatusInfo, RabbitResponseQueueConsumer}
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.typesafe.config.Config

import scala.concurrent.Await


/**
 * Created by jpetty on 8/28/15.
 */
final class HyppoCoordinator @Singleton() @Inject() (system: ActorSystem, config: CoordinatorConfig, dispatcher: WorkDispatcher, handler: WorkResponseHandler) extends WorkDispatcher {

  private val responseActor = system.actorOf(Props(classOf[RabbitResponseQueueConsumer], config, handler), name = "responses")

  system.registerOnTermination({
    Await.result(gracefulStop(responseActor, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown), config.rabbitMQTimeout)
  })

  override def enqueue(work: WorkerInput) : Unit = dispatcher.enqueue(work)

  override def fetchQueueStatuses() : Seq[QueueStatusInfo] = dispatcher.fetchQueueStatuses()

}



object HyppoCoordinator {

  def apply(system: ActorSystem, config: CoordinatorConfig, dispatcher: WorkDispatcher, handler: WorkResponseHandler) : HyppoCoordinator  = {
    new HyppoCoordinator(system, config, dispatcher, handler)
  }

  def apply(system: ActorSystem, dispatcher: WorkDispatcher, handler: WorkResponseHandler) : HyppoCoordinator = {
    val config = createConfig(system.settings.config)
    apply(system, config, dispatcher, handler)
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
}
