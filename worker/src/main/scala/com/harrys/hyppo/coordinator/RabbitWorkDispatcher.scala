package com.harrys.hyppo.coordinator

import javax.inject.{Inject, Singleton}

import akka.actor.{ActorSystem, Props}
import akka.pattern.gracefulStop
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.worker.actor.amqp.{QueueStatusInfo, RabbitWorkQueueProxy}
import com.harrys.hyppo.worker.api.proto.WorkerInput

import scala.concurrent.Await

/**
 * Created by jpetty on 9/28/15.
 */

final class RabbitWorkDispatcher @Singleton() @Inject() (system: ActorSystem, config: CoordinatorConfig) extends WorkDispatcher {

  private val enqueueActor = system.actorOf(Props(classOf[RabbitWorkQueueProxy], config), name = "enqueue")
  private val rabbitMQApi  = config.newRabbitMQApiClient()

  system.registerOnTermination({
    Await.ready(gracefulStop(enqueueActor, config.rabbitMQTimeout), config.rabbitMQTimeout)
  })

  override def enqueue(input: WorkerInput): Unit = {
    enqueueActor ! input
  }

  override def fetchQueueStatuses(): Seq[QueueStatusInfo] = rabbitMQApi.fetchQueueStatusInfo()
}
