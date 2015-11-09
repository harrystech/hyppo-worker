package com.harrys.hyppo.worker.actor.amqp

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.RabbitMQTests

/**
 * Created by jpetty on 9/16/15.
 */
class QueueStatusUpdateActorTests extends RabbitMQTests("QueueStatusUpdateActorTests", TestConfig.workerWithRandomQueuePrefix()) {

  "The QueueStatusActor" must {
    val status = TestActorRef(new RabbitQueueStatusActor(config, self))

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
