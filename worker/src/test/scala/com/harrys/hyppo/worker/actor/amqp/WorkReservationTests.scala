package com.harrys.hyppo.worker.actor.amqp

import java.util.UUID

import akka.actor.{PoisonPill, ActorRef}
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.harrys.hyppo.config.{CoordinatorConfig, WorkerConfig}
import com.harrys.hyppo.source.api.LogicalOperation
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.QueueStatusUpdate
import com.harrys.hyppo.worker.actor.data.LocalJarLoadingActor
import com.harrys.hyppo.worker.actor.queue.{WorkQueueExecution, WorkDelegation}
import com.harrys.hyppo.worker.api.proto.CreateIngestionTasksRequest
import com.harrys.hyppo.worker.{BlockingProcessedDataStub, TestObjects, TestConfig}
import com.harrys.hyppo.worker.actor.{WorkerFSM, RequestForAnyWork, RabbitMQTests}
import org.scalatest.concurrent.Eventually

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Created by jpetty on 2/8/16.
  */
class WorkReservationTests extends RabbitMQTests("WorkerDelegatorActorTests", TestConfig.workerWithRandomQueuePrefix()) with Eventually {
  import com.thenewmotion.akka.rabbitmq._

  this.beforeAll()

  val rabbitHttp   = config.newRabbitMQApiClient()
  val channelActor = createChannelActor()
  val work         = createWorkToTest()
  val queueName    = enqueueWork(work)

  val delegationActor = TestActorRef(new WorkDelegation(config))
  //  Create the WorkerFSM but disable the timer
  val jarLoading      = TestActorRef(new LocalJarLoadingActor())
  val workerFSMActor  = TestActorRef(new WorkerFSM(config, delegationActor, connectionActor, jarLoading))
  workerFSMActor.underlyingActor.cancelTimer(WorkerFSM.PollingTimerName)


  "The Delegator" must {

    "start with the enqueue work" in {
      eventually {
        fetchQueueDetails().size shouldEqual 1
      }(PatienceConfig(timeout = config.rabbitMQTimeout))
      delegationActor ! fetchQueueStatus()
    }

    "reserve the work when requested" in {
      workerFSMActor ! WorkerFSM.RequestWorkEvent
      eventually {
        fetchQueueDetails().unacknowledged shouldEqual 1
      }(PatienceConfig(timeout = config.rabbitMQTimeout))
    }

    "release the work automatically when the channel closes" in {
      workerFSMActor.stop()
      eventually {
        val details = fetchQueueDetails()
        details.size shouldEqual 1
        details.unacknowledged shouldEqual 0
      }(PatienceConfig(timeout = config.workerShutdownTimeout.plus(config.rabbitMQTimeout)))
    }
  }

  def fetchQueueStatus(): QueueStatusUpdate = {
    QueueStatusUpdate(Seq(fetchQueueDetails()))
  }

  def fetchQueueDetails(): SingleQueueDetails = {
    val details = rabbitHttp.fetchRawHyppoQueueDetails()
    details.find(_.queueName == queueName).getOrElse(throw new IllegalArgumentException(s"No queue exists with name: $queueName"))
  }

  def createWorkToTest(): CreateIngestionTasksRequest = {
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
