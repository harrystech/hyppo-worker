package com.harrys.hyppo

import akka.actor._
import akka.pattern.gracefulStop
import com.google.inject.{Guice, Injector}
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
final class HyppoWorker private(val system: ActorSystem, val settings: WorkerConfig, override val injector: Injector) extends ActorInject {

  //  Kick off baseline queue creation
  HyppoCoordinator.initializeBaseQueues(settings)

  val connection = system.actorOf(ConnectionActor.props(settings.rabbitMQConnectionFactory, reconnectionDelay = settings.rabbitMQTimeout), name = "rabbitmq")

  val delegation = createDelegationActor()
  val workerFSMs = (1 to settings.workerCount).inclusive.map(i => createWorker(i))


  system.registerOnTermination({
    delegation ! Lifecycle.ImpendingShutdown
    val workerWait  = FiniteDuration(settings.shutdownTimeout.mul(0.8).toMillis, MILLISECONDS)
    val futures     = workerFSMs.map(ref => gracefulStop(ref, workerWait, Lifecycle.ImpendingShutdown))
    implicit val ec = system.dispatcher
    Await.result(Future.sequence(futures), settings.shutdownTimeout)
  })

  def awaitSystemTermination() : Unit = system.awaitTermination()

  def createDelegationActor(): ActorRef = {
    implicit val topLevel = system
    injectActor(injector.getInstance(classOf[WorkDelegation]), "delegation")
  }

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
    apply(system, config, Guice.createInjector(), new HyppoWorkerModule(system, config))
  }

  def apply(system: ActorSystem, config: WorkerConfig, injector: Injector): HyppoWorker = {
    apply(system, config, injector, new HyppoWorkerModule(system, config))
  }

  def apply[M <: HyppoWorkerModule](system: ActorSystem, config: WorkerConfig, module: M): HyppoWorker = {
    apply(system, config, Guice.createInjector(), module)
  }

  def apply[M <: HyppoWorkerModule](system: ActorSystem, config: WorkerConfig, injector: Injector, module: M): HyppoWorker = {
    val child = injector.createChildInjector(module)
    new HyppoWorker(system, config, child)
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
