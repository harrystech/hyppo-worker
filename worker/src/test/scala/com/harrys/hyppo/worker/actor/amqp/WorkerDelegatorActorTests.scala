package com.harrys.hyppo.worker.actor.amqp

import java.time.LocalDateTime
import java.util.UUID

import akka.testkit.{TestActorRef, TestProbe}
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.queue.{WorkDelegation, WorkQueueExecution}
import com.harrys.hyppo.worker.actor.{RabbitMQTests, RequestForAnyWork, RequestForPreferredWork}
import com.harrys.hyppo.worker.api.proto.CreateIngestionTasksRequest
import com.harrys.hyppo.worker.{TestConfig, TestObjects}

import scala.util.Try

/**
 * Created by jpetty on 9/16/15.
 */
class WorkerDelegatorActorTests extends RabbitMQTests("WorkerDelegatorActorTests", TestConfig.workerWithRandomQueuePrefix())  {
  import com.thenewmotion.akka.rabbitmq._

  "The WorkDelegator" must {

    val delegator  = TestActorRef(new WorkDelegation(config))

    "initialize with empty queue status information" in {
      delegator.underlyingActor.currentStats shouldBe empty
    }

    "respond to queue status updates by updating it status info" in {
      val statuses = Seq(QueueStatusInfo(name = naming.generalQueueName, size = 0, rate = 0.0, LocalDateTime.now()))
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(statuses)
      delegator.underlyingActor.currentStats shouldEqual statuses.map(s => s.name -> s).toMap
    }

    "incrementally update the queue statuses as new information arrives" in {
      val channel = connectionActor.createChannel(ChannelActor.props(), name = Some("partial-test"))

      val integrations = Seq(
        TestObjects.testProcessedDataIntegration(TestObjects.testIngestionSource(name = "Test Source One")),
        TestObjects.testProcessedDataIntegration(TestObjects.testIngestionSource(name = "Test Source Two"))
      )
      val workItems = integrations.map { integration =>
        CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), TestObjects.testIngestionJob(integration.source))
      }
      val queues = workItems.map { item =>
        enqueueWork(item)
        QueueStatusInfo(name = naming.integrationWorkQueueName(item), size = 1, rate = 0.0, idleSince = TimeUtils.currentLocalDateTime())
      }
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(queues)
      delegator.underlyingActor.currentStats shouldEqual queues.map(i => i.name -> i).toMap

      //  Clear the contents of those queues
      withChannel { c =>
        queues.map(_.name).foreach(c.queuePurge)
      }

      delegator ! RequestForAnyWork(channel)
      expectNoMsg()
      delegator.underlyingActor.currentStats.mapValues(_.size) shouldEqual queues.map(i => i.name -> 0).toMap
    }

    "provide preferred work when possible" in {
      val integration = TestObjects.testProcessedDataIntegration(TestObjects.testIngestionSource(name = "work delegator"))
      val testJob     = TestObjects.testIngestionJob(integration.source)
      val work        = CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), testJob)
      val workerChan  = connectionActor.createChannel(ChannelActor.props())

      val queueName   = enqueueWork(work)
      try {
        val probe = TestProbe()
        delegator ! RabbitQueueStatusActor.QueueStatusUpdate(Seq(QueueStatusInfo(name = queueName, size = 1, rate = 0.0, idleSince = LocalDateTime.now())))
        probe.send(delegator, RequestForPreferredWork(workerChan, integration))
        val reply = probe.expectMsgType[WorkQueueExecution]
        reply.input shouldBe a[CreateIngestionTasksRequest]
        reply.input.code.isSameCode(integration.code) shouldBe true

      } finally {
        Try(connection.close())
      }
    }
  }
}
