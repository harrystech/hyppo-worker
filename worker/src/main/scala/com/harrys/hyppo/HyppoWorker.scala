package com.harrys.hyppo

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.amqp.{RabbitWorkerDelegation, RabbitQueueStatusActor}
import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by jpetty on 8/28/15.
 */
final class HyppoWorker(val system: ActorSystem, config: Config) {
  val settings   = new WorkerConfig(config.resolve())
  val delegation = system.actorOf(Props(classOf[RabbitWorkerDelegation], settings), "delegation")
  val queueStats = system.actorOf(Props(classOf[RabbitQueueStatusActor], settings, delegation), "queue-stats")
  val workerFSM  = system.actorOf(Props(classOf[WorkerFSM], settings, delegation), "worker")

  system.registerOnTermination({
    delegation ! Lifecycle.ImpendingShutdown
    queueStats ! PoisonPill
    Await.result(gracefulStop(workerFSM, Duration(6, SECONDS), Lifecycle.ImpendingShutdown), Duration(8, SECONDS))
  })
}

object HyppoWorker {

  def apply(system: ActorSystem): HyppoWorker = {
    val config = system.settings.config
      .withFallback(referenceConfig())
      .resolve()

    new HyppoWorker(system, config)
  }

  def referenceConfig(): Config = ConfigUtils.resourceFileConfig("/com/harrys/hyppo/config/reference.conf")
}
