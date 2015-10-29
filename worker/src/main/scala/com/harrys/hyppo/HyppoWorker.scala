package com.harrys.hyppo

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.amqp.{RabbitQueueStatusActor, RabbitWorkerDelegation}
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Created by jpetty on 8/28/15.
 */
final class HyppoWorker(val system: ActorSystem, val settings: WorkerConfig) {

  def this(system: ActorSystem, config: Config) = this(system, new WorkerConfig(config))

  val delegation = system.actorOf(Props(classOf[RabbitWorkerDelegation], settings), "delegation")
  val queueStats = system.actorOf(Props(classOf[RabbitQueueStatusActor], settings, delegation), "queue-stats")
  val workerFSMs = (1 to settings.workerCount).inclusive.map(i => {
    system.actorOf(Props(classOf[WorkerFSM], settings, delegation), "worker-%02d".format(i))
  })

  system.registerOnTermination({
    delegation ! Lifecycle.ImpendingShutdown
    queueStats ! PoisonPill
    val futures = workerFSMs.map(ref => gracefulStop(ref, Duration(6, SECONDS), Lifecycle.ImpendingShutdown))
    implicit val ec = system.dispatcher
    Await.result(Future.sequence(futures), Duration(8, SECONDS))
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
