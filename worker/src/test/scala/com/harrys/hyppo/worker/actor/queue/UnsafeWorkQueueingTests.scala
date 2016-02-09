package com.harrys.hyppo.worker.actor.queue

import java.util.UUID

import akka.actor.ActorRef
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.QueueStatusUpdate
import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails
import com.harrys.hyppo.worker.actor.{RabbitMQTests, WorkerFSM}
import com.harrys.hyppo.worker.api.proto.{PersistProcessedDataRequest, RemoteProcessedDataFile, RemoteStorageLocation}
import com.harrys.hyppo.worker.{BlockingProcessedDataStub, TestConfig, TestObjects}
import org.scalatest.concurrent.Eventually

/**
  * Created by jpetty on 2/9/16.
  */
class UnsafeWorkQueueingTests extends RabbitMQTests("UnsafeWorkQueueingTests", TestConfig.workerWithRandomQueuePrefix()) with Eventually {
  import com.thenewmotion.akka.rabbitmq._

  val rabbitHttp   = config.newRabbitMQApiClient()
  val channelActor = createChannelActor()


  "The Workers" when {
    "handling unsafe work" must {
      val work         = createUnsafeWork()
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

      "not send the work back to the queue if failure occurs" in {

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

  def createUnsafeWork(): PersistProcessedDataRequest = {
    val source      =  TestObjects.testIngestionSource(name = "reservation tests")
    val integration = TestObjects.testExecutableIntegration(source, new BlockingProcessedDataStub())
    val testJob     = TestObjects.testIngestionJob(source)
    val testTask    = TestObjects.testIngestionTask(testJob)
    val testFile    = RemoteProcessedDataFile(RemoteStorageLocation("LOCAL TEST", ""), 0L, new Array[Byte](0), 0L)
    PersistProcessedDataRequest(integration, UUID.randomUUID(), Seq(), testTask, testFile)
  }

  def createChannelActor(): ActorRef = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    connectionActor.createChannel(ChannelActor.props())
  }
}
