package com.harrys.hyppo

import akka.actor._
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.ConfigUtils
import com.harrys.hyppo.worker.actor.WorkerFSM
import com.harrys.hyppo.worker.actor.amqp.{RabbitQueueStatusActor, RabbitWorkerDelegation}
import com.typesafe.config.Config

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

/**
 * Created by jpetty on 8/28/15.
 */
final class HyppoWorker(val system: ActorSystem, config: Config) {
  val settings   = new WorkerConfig(config.resolve())
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
