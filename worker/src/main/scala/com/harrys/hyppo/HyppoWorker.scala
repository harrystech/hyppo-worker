package com.harrys.hyppo

import akka.actor._
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.amqp.{QueueHelpers, RabbitQueueStatusActor}
import com.harrys.hyppo.worker.actor.queue.WorkDelegation
import com.thenewmotion.akka.rabbitmq._
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Created by jpetty on 8/28/15.
 */
final class HyppoWorker(val system: ActorSystem, val settings: WorkerConfig) {

  def this(system: ActorSystem, config: Config) = this(system, new WorkerConfig(config))

  val connection = system.actorOf(ConnectionActor.props(settings.rabbitMQConnectionFactory, reconnectionDelay = settings.rabbitMQTimeout), name = "rabbitmq")
  //  Kick off baseline queue creation
  HyppoCoordinator.initializeBaseQueues(settings, system, connection)
  val delegation = system.actorOf(Props(classOf[WorkDelegation], settings), "delegation")
  val queueStats = system.actorOf(Props(classOf[RabbitQueueStatusActor], settings, delegation), "queue-stats")
  val workerFSMs = (1 to settings.workerCount).inclusive.map(i => {
    system.actorOf(Props(classOf[WorkerFSM], settings, delegation, connection), "worker-%02d".format(i))
  })



  system.registerOnTermination({
    delegation ! Lifecycle.ImpendingShutdown
    queueStats ! PoisonPill
    val workerWait  = FiniteDuration(settings.shutdownTimeout.mul(0.8).toMillis, MILLISECONDS)
    val futures     = workerFSMs.map(ref => gracefulStop(ref, workerWait, Lifecycle.ImpendingShutdown))
    implicit val ec = system.dispatcher
    Await.result(Future.sequence(futures), settings.shutdownTimeout)
  })

  def awaitSystemTermination() : Unit = system.awaitTermination()
}

object HyppoWorker {

  def apply(system: ActorSystem): HyppoWorker = {
    val config = createConfig(system.settings.config)
    new HyppoWorker(system, config)
  }

  def createConfig(appConfig: Config) : WorkerConfig = {
    val config = appConfig.withFallback(referenceConfig())

    val merged = requiredConfig().
      withFallback(config)
      .resolve()

    new WorkerConfig(merged)
  }

  def requiredConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/required.conf")

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")
}
