package com.harrys.hyppo.worker.actor.queue

import java.util.UUID

import akka.actor.ActorRef
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.QueueStatusUpdate
import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails
import com.harrys.hyppo.worker.actor.{RabbitMQTests, WorkerFSM}
import com.harrys.hyppo.worker.api.proto.CreateIngestionTasksRequest
import com.harrys.hyppo.worker.{BlockingProcessedDataStub, TestConfig, TestObjects}
import org.scalatest.concurrent.Eventually

/**
  * Created by jpetty on 2/8/16.
  */
class IdempotentWorkQueueingTests extends RabbitMQTests("IdempotentWorkQueueingTests", TestConfig.workerWithRandomQueuePrefix()) with Eventually {
  import com.thenewmotion.akka.rabbitmq._

  val rabbitHttp   = config.newRabbitMQApiClient()
  val channelActor = createChannelActor()


  "The Workers" when {
    "handling idempotent work" must {
      val work         = createIdempotentWork()
      val queueName    = enqueueWork(work)

      val delegationActor = TestActorRef(new WorkDelegation(config))
      //  Create the WorkerFSM but disable the timer
      val jarLoading      = TestActorRef(new LocalJarLoadingActor())
      val workerFSMActor  = TestActorRef(new WorkerFSM(config, delegationActor, connectionActor, jarLoading))
      workerFSMActor.underlyingActor.cancelTimer(WorkerFSM.PollingTimerName)

      "find the pending work" in {
        eventually {
          val details = fetchQueueDetails(queueName)
          details.size shouldEqual 1
          details.unacknowledged shouldEqual 0
        }(PatienceConfig(timeout = config.rabbitMQTimeout))
        delegationActor ! fetchQueueStatus(queueName)
      }

      "reserve the work from the queue" in {
        workerFSMActor ! WorkerFSM.RequestWorkEvent
        eventually {
          fetchQueueDetails(queueName).unacknowledged shouldEqual 1
        }(PatienceConfig(timeout = config.rabbitMQTimeout))
      }

      "return the work to the queue when a failure occurs outside of the commander" in {
        workerFSMActor.stop()
        eventually {
          val details = fetchQueueDetails(queueName)
          details.size shouldEqual 1
          details.unacknowledged shouldEqual 0
        }(PatienceConfig(timeout = config.workerShutdownTimeout.plus(config.rabbitMQTimeout)))
      }
    }
  }

  def fetchQueueStatus(queueName: String): QueueStatusUpdate = {
    QueueStatusUpdate(Seq(fetchQueueDetails(queueName)))
  }

  def fetchQueueDetails(queueName: String): SingleQueueDetails = {
    val details = rabbitHttp.fetchRawHyppoQueueDetails()
    details.find(_.queueName == queueName).getOrElse(throw new IllegalArgumentException(s"No queue exists with name: $queueName"))
  }

  def createIdempotentWork(): CreateIngestionTasksRequest = {
    val source      =  TestObjects.testIngestionSource(name = "reservation tests")
    val integration = TestObjects.testExecutableIntegration(source, new BlockingProcessedDataStub())
    val testJob     = TestObjects.testIngestionJob(source)
    CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), testJob)
  }

  def createChannelActor(): ActorRef = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    connectionActor.createChannel(ChannelActor.props())
  }
}
