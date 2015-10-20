package com.harrys.hyppo.worker.actor.amqp

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.TestConfig

/**
 * Created by jpetty on 9/16/15.
 */
class QueueStatusUpdateActorTests extends RabbitMQTests {

  "The QueueStatusActor" must {
    val status = TestActorRef(new RabbitQueueStatusActor(new WorkerConfig(TestConfig.basicTestConfig), self))

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
