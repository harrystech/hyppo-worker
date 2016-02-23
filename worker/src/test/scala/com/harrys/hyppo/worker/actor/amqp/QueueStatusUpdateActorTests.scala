package com.harrys.hyppo.worker.actor.amqp

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import com.google.inject.Guice
import com.harrys.hyppo.worker.actor.RabbitMQTests
import com.harrys.hyppo.worker.{TestConfig, WorkerLocalTestModule}

/**
 * Created by jpetty on 9/16/15.
 */
class QueueStatusUpdateActorTests extends RabbitMQTests("QueueStatusUpdateActorTests", TestConfig.workerWithRandomQueuePrefix()) {

  val module = new WorkerLocalTestModule(system, config) {
    override protected def bindRabbitQueueStatusActorFactory(): Unit = {
      bindActorFactory[RabbitQueueStatusActor, RabbitQueueStatusActor.Factory]
    }
  }
  val injector = Guice.createInjector(module)

  "The QueueStatusActor" must {
    val factory = injector.getInstance(classOf[RabbitQueueStatusActor.Factory])
    val status  = TestActorRef(factory(self).asInstanceOf[RabbitQueueStatusActor], "queue-status")

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
