package com.harrys.hyppo

import javax.inject.{Inject, Singleton}

import akka.actor._
import akka.pattern.gracefulStop
import com.codahale.metrics.MetricRegistry
import com.google.inject.{Guice, Injector, Provider}
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.amqp.QueueHelpers
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
  injectorProvider: Provider[Injector],
  system:           ActorSystem,
  config:           WorkerConfig,
  workerFactory:    WorkerFSM.Factory
) extends ActorInject {

  override def injector: Injector = injectorProvider.get()

  val connection = system.actorOf(ConnectionActor.props(config.rabbitMQConnectionFactory, reconnectionDelay = config.rabbitMQTimeout, initializeConnection), name = "rabbitmq")

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

  private def createWorker(number: Int): ActorRef = {
    implicit val topLevel = system
    injectActor(workerFactory(delegation, connection), "worker-%02d".format(number))
  }

  private def initializeConnection(connection: Connection, actor: ActorRef): Unit = {
    val helpers = new QueueHelpers(config)
    helpers.initializeRequiredQueues(connection)
  }
}

object HyppoWorker {

  def apply(system: ActorSystem): HyppoWorker = {
    apply(system, createConfig(system.settings.config))
  }

  def apply[M <: HyppoWorkerModule](system: ActorSystem, config: WorkerConfig): HyppoWorker = {
    val module = new HyppoWorkerModule {
      override protected def configureSpecializedBindings(): Unit = {
        bind(classOf[ActorSystem]).toInstance(system)
        bind(classOf[WorkerConfig]).toInstance(config)
        bind(classOf[MetricRegistry]).asEagerSingleton()
      }
    }
    val injector = Guice.createInjector(module)
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
