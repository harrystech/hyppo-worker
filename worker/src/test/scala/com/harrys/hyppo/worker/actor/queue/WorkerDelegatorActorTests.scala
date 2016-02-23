package com.harrys.hyppo.worker.actor.queue

import java.time.LocalDateTime
import java.util.UUID

import akka.testkit.{TestActorRef, TestProbe}
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.{RabbitQueueStatusActor, SingleQueueDetails}
import com.harrys.hyppo.worker.actor.{RabbitMQTests, RequestForAnyWork, RequestForPreferredWork}
import com.harrys.hyppo.worker.api.proto.CreateIngestionTasksRequest
import com.harrys.hyppo.worker.{TestConfig, TestObjects}

import scala.util.Try

/**
 * Created by jpetty on 9/16/15.
 */
class WorkerDelegatorActorTests extends RabbitMQTests("WorkerDelegatorActorTests", TestConfig.workerWithRandomQueuePrefix())  {
  import com.thenewmotion.akka.rabbitmq._

  val injector = TestConfig.localWorkerInjector(system, config)

  "The WorkDelegator" must {

    val delegator  = TestActorRef(injector.getInstance(classOf[WorkDelegation]), "delegation")

    "initialize with empty queue status information" in {
      delegator.underlyingActor.statusTracker.integrationQueueMetrics() shouldBe empty
    }

    "respond to queue status updates by updating it status info" in {
      val statuses = Seq(SingleQueueDetails(queueName = naming.generalQueueName, size = 0, rate = 0.0, ready = 0, unacknowledged = 0, LocalDateTime.now()))
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(statuses)
      delegator.underlyingActor.statusTracker.generalQueueMetrics().details shouldEqual statuses.head
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
        SingleQueueDetails(queueName = naming.integrationWorkQueueName(item), size = 1, rate = 0.0, ready = 1, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime())
      }
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(queues)
      val metrics = delegator.underlyingActor.statusTracker.integrationQueueMetrics()
      metrics.size shouldEqual 2
      metrics.map(_.details).sortBy(_.queueName) shouldEqual queues.sortBy(_.queueName)

      //  Clear the contents of those queues
      withChannel { c =>
        queues.map(_.queueName).foreach(c.queuePurge)
      }

      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(Seq())
      delegator ! RequestForAnyWork(channel)
      expectNoMsg()
      delegator.underlyingActor.statusTracker.integrationQueueMetrics().size shouldEqual 0
    }

    "provide preferred work when possible" in {
      val integration = TestObjects.testProcessedDataIntegration(TestObjects.testIngestionSource(name = "work delegator"))
      val testJob     = TestObjects.testIngestionJob(integration.source)
      val work        = CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), testJob)
      val workerChan  = connectionActor.createChannel(ChannelActor.props())

      val queueName   = enqueueWork(work)
      try {
        val probe  = TestProbe()
        val queues = Seq(SingleQueueDetails(queueName  = queueName, size = 1, rate = 0.0, ready = 1, unacknowledged = 0, idleSince = LocalDateTime.now()))
        delegator ! RabbitQueueStatusActor.QueueStatusUpdate(queues)
        delegator.underlyingActor.statusTracker.integrationQueueMetrics().size shouldEqual 1
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
