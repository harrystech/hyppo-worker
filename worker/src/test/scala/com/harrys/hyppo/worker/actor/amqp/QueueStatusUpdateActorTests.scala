package com.harrys.hyppo.worker.actor.amqp

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.RabbitMQTests

/**
 * Created by jpetty on 9/16/15.
 */
class QueueStatusUpdateActorTests extends RabbitMQTests("QueueStatusUpdateActorTests", TestConfig.workerWithRandomQueuePrefix()) {

  val injector = TestConfig.localWorkerInjector(system, config)

  "The QueueStatusActor" must {
    val factory = injector.getInstance(classOf[RabbitQueueStatusActor.Factory])
    val status  = TestActorRef(factory(self), "queue-status")

    "schedule a timer to refresh queue information" in {
      status.underlyingActor.statusTimer.isCancelled shouldBe false
    }

    "cancel the refresh timer upon stopping" in {
      val timer = status.underlyingActor.statusTimer
      status ! PoisonPill
      timer.isCancelled shouldBe true
    }
  }
}
