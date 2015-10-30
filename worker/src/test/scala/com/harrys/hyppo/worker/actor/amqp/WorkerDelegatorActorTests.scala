package com.harrys.hyppo.worker.actor.amqp

import java.time.LocalDateTime

import akka.testkit.{TestActorRef, TestProbe}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.{RequestForAnyWork, RequestForPreferredWork}
import com.harrys.hyppo.worker.api.proto.{CreateIngestionTasksRequest, FailureResponse, RemoteLogFile}
import com.harrys.hyppo.worker.{TestConfig, TestObjects}

import scala.util.Try

/**
 * Created by jpetty on 9/16/15.
 */
class WorkerDelegatorActorTests extends RabbitMQTests  {

  val config = new WorkerConfig(TestConfig.basicTestConfig)

  "The WorkDelegator" must {

    val delegator  = TestActorRef(new RabbitWorkerDelegation(config))
    val serializer = new AMQPSerialization(system)

    "initialize with empty queue status information" in {
      delegator.underlyingActor.currentStats shouldBe empty
    }

    "respond to queue status updates by updating it status info" in {
      val statuses = Seq(QueueStatusInfo(name = "test", size = 0, rate = 0.0, LocalDateTime.now()))
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(statuses)
      delegator.underlyingActor.currentStats shouldEqual statuses
    }

    "respond with no work when the queues are empty" in {
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(Seq())
      delegator ! RequestForAnyWork
      expectNoMsg()
    }

    "provide preferred work when possible" in {
      val integration = TestObjects.testProcessedDataIntegration(TestObjects.testIngestionSource(name = "work delegator"))
      val work        = CreateIngestionTasksRequest(integration, TestObjects.testIngestionJob())
      val connection  = config.rabbitMQConnectionFactory.newConnection()
      val channel     = connection.createChannel()

      val queueName = HyppoQueue.integrationQueueName(integration)
      channel.queueDelete(queueName)
      channel.queueDeclare(queueName, false, false, false, null)
      channel.basicPublish("", queueName, null, serializer.serialize(work))
      try {
        val probe = TestProbe()
        delegator ! RabbitQueueStatusActor.QueueStatusUpdate(Seq(QueueStatusInfo(name = queueName, size = 1, rate = 0.0, idleSince = LocalDateTime.now())))

        probe.send(delegator, RequestForPreferredWork(integration))
        val reply = probe.expectMsgType[WorkQueueItem]
        reply.input shouldBe a[CreateIngestionTasksRequest]

        probe.reply(FailureResponse(work, RemoteLogFile("", ""), None))

        reply.input.code.isSameCode(integration.code) shouldBe true

      } finally {
        Try(channel.queueDelete(queueName))
        Try(channel.close())
        Try(connection.close())
      }
    }
  }
}
