package com.harrys.hyppo.worker.actor.amqp

import java.util.UUID

import akka.testkit.TestActorRef
import com.harrys.hyppo.worker.actor.RabbitMQTests
import com.harrys.hyppo.worker.api.proto.CreateIngestionTasksRequest
import com.harrys.hyppo.worker.{TestConfig, TestObjects}

/**
 * Created by jpetty on 9/17/15.
 */
class QueueProxyActorTests extends RabbitMQTests("QueueProxyActorTests", TestConfig.coordinatorWithRandomQueuePrefix())  {


  "The Queue Proxy" must {
    val proxy = TestActorRef(new EnqueueWorkQueueProxy(config, connectionActor))

    "successfully enqueue messages" in {
      val source      =  TestObjects.testIngestionSource(name = "queue proxy")
      val integration = TestObjects.testProcessedDataIntegration(source)
      val testJob     = TestObjects.testIngestionJob(source)
      val work        = CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), testJob)
      val queueName   = naming.integrationWorkQueueName(work)

      proxy ! work
      within(config.rabbitMQTimeout){
        awaitAssert(helpers.checkQueueSize(connection, queueName) shouldBe 1)
      }
    }
  }
}
