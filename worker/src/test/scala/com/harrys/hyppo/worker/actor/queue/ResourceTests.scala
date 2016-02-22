package com.harrys.hyppo.worker.actor.queue

import java.util.UUID

import akka.testkit.TestActorRef
import com.harrys.hyppo.worker.actor.amqp.{RabbitHttpClient, RabbitQueueStatusActor}
import com.harrys.hyppo.worker.actor.{RabbitMQTests, WorkerFSM}
import com.harrys.hyppo.worker.api.proto.{CreateIngestionTasksRequest, WorkResource}
import com.harrys.hyppo.worker.{BlockingProcessedDataStub, TestConfig, TestObjects}
import org.scalatest.concurrent.Eventually

/**
  * Created by jpetty on 2/9/16.
  */
class ResourceTests extends RabbitMQTests(
  "ResourceTests",
  TestConfig.workerWithRandomQueuePrefix().withValue("hyppo.worker.task-polling-interval", "200ms")
) with Eventually {

  override implicit def patienceConfig = PatienceConfig(timeout = config.rabbitMQTimeout)

  val injector      = TestConfig.localWorkerInjector(system, config)
  val httpClient    = injector.getInstance(classOf[RabbitHttpClient])
  val workerFactory = injector.getInstance(classOf[WorkerFSM.Factory])

  val resource  = naming.concurrencyResource("Concurrency Resource", 1)
  val workQueue = naming.integrationWorkQueueName(createResourceWorkRequest(resource))

  "Work Requiring Resources" must {
    val delegator = TestActorRef(injector.getInstance(classOf[WorkDelegation]), "delegation")

    "successfully create the resource queue" in {
      enqueueWork(createResourceWorkRequest(resource))
      eventually {
        val detail = httpClient.fetchRawQueueDetails().find(_.queueName == resource.queueName)
        detail.isDefined shouldBe true
        detail.get.size shouldBe 1
      }
    }

    "populate multiple tasks to it" in {
      (0 to 10).foreach { _ => enqueueWork(createResourceWorkRequest(resource)) }
      eventually {
        val detail = httpClient.fetchRawQueueDetails().find(_.queueName == workQueue)
        detail.isDefined shouldBe true
        detail.get.size shouldBe 12
      }
    }

    "successfully acquire the work resource once" in {
      delegator ! RabbitQueueStatusActor.QueueStatusUpdate(httpClient.fetchRawHyppoQueueDetails())
      val workerOne = TestActorRef(workerFactory(delegator, connectionActor), "worker-1")
      eventually {
        val reserved = httpClient.fetchRawHyppoQueueDetails().find(_.queueName == workQueue).map(_.unacknowledged)
        reserved.isDefined shouldBe true
        reserved.get shouldBe 1
      }
    }
  }

  def createResourceWorkRequest(resource: WorkResource): CreateIngestionTasksRequest = {
    val source   = TestObjects.testIngestionSource("Resource Tests")
    val job      = TestObjects.testIngestionJob(source = source)
    val exec     = TestObjects.testExecutableIntegration(source, new BlockingProcessedDataStub())
    CreateIngestionTasksRequest(exec, UUID.randomUUID(), Seq(resource), job)
  }
}
