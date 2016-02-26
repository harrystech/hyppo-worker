package com.harrys.hyppo

import javax.inject.{Singleton, Inject}

import akka.actor._
import akka.pattern.gracefulStop
import com.google.inject.{Provider, Guice, Injector}
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.queue.WorkDelegation
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq._
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Created by jpetty on 8/28/15.
 */
@Singleton
final class HyppoWorker @Inject()
(
  system:           ActorSystem,
  config:           WorkerConfig,
  injectorProvider: Provider[Injector]
) extends ActorInject {

  override def injector: Injector = injectorProvider.get()

  //  Kick off baseline queue creation
  HyppoCoordinator.initializeBaseQueues(config)

  val connection = system.actorOf(ConnectionActor.props(config.rabbitMQConnectionFactory, reconnectionDelay = config.rabbitMQTimeout), name = "rabbitmq")

  val delegation = injectTopActor[WorkDelegation]("delegation")
  val workerFSMs = (1 to config.workerCount).inclusive.map(i => createWorker(i))

  system.registerOnTermination({
    delegation ! Lifecycle.ImpendingShutdown
    val workerWait  = FiniteDuration(config.shutdownTimeout.mul(0.8).toMillis, MILLISECONDS)
    val futures     = workerFSMs.map(ref => gracefulStop(ref, workerWait, Lifecycle.ImpendingShutdown))
    implicit val ec = system.dispatcher
    Await.result(Future.sequence(futures), config.shutdownTimeout)
  })

  def awaitSystemTermination() : Unit = system.awaitTermination()

  def createWorker(number: Int): ActorRef = {
    implicit val topLevel = system
    val name    = "worker-%02d".format(number)
    val factory = injector.getInstance(classOf[WorkerFSM.Factory])
    injectActor(factory(delegation, connection), name)
  }
}

object HyppoWorker {

  def apply(system: ActorSystem): HyppoWorker = {
    apply(system, createConfig(system.settings.config))
  }

  def apply(system: ActorSystem, config: WorkerConfig): HyppoWorker = {
    apply(system, config, new HyppoWorkerModule(system, config))
  }

  def apply[M <: HyppoWorkerModule](system: ActorSystem, config: WorkerConfig, module: M): HyppoWorker = {
    val injector = Guice.createInjector(new HyppoWorkerModule(system, config))
    injector.getInstance(classOf[HyppoWorker])
  }

  def createConfig(appConfig: Config): WorkerConfig = {
    val config = appConfig.withFallback(referenceConfig())
    val merged = requiredConfig()
      .withFallback(config)
      .resolve()
    new WorkerConfig(merged)
  }

  def requiredConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/required.conf")

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")
}
