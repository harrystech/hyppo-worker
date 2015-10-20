package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.{WorkResponseHandler, WorkDispatcher}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.amqp.{RabbitResponseQueueConsumer, QueueStatusInfo}
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
    apply(system, settingsFromActorSystem(system), dispatcher, handler)
  }

  def settingsFromActorSystem(system: ActorSystem) : CoordinatorConfig = {
    val config = system.settings.config
      .withFallback(referenceConfig())
      .resolve()

    new CoordinatorConfig(config)
  }

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")
}
