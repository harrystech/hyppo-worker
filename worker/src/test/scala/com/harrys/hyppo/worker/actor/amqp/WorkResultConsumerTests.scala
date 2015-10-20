package com.harrys.hyppo.worker.actor.amqp

import akka.pattern.gracefulStop
import akka.testkit.TestActorRef
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.coordinator.WorkResponseHandler
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.api.proto._

import scala.concurrent.Await

/**
 * Created by jpetty on 9/17/15.
 */
class WorkResultConsumerTests extends RabbitMQTests {

  val config = new CoordinatorConfig(TestConfig.basicTestConfig)

  val handler = new WorkResponseHandler {
    override def onIngestionTasksCreated(created: CreateIngestionTasksResponse): Unit = {}

    override def onWorkFailed(failure: FailureResponse): Unit = {}

    override def onRawDataProcessed(processed: ProcessRawDataResponse): Unit = {}

    override def onProcessedDataFetched(fetched: FetchProcessedDataResponse): Unit = {}

    override def onIntegrationValidated(validated: ValidateIntegrationResponse): Unit = {}

    override def onRawDataFetched(fetched: FetchRawDataResponse): Unit = {}

    override def onProcessedDataPersisted(persisted: PersistProcessedDataResponse): Unit = {}
  }

  "The WorkResultConsumer" must {
    val consumer = TestActorRef(new RabbitResponseQueueConsumer(config, handler))

    "gracefully shutdown when told" in {
      val future = gracefulStop(consumer, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown)
      Await.result(future, config.rabbitMQTimeout) shouldBe true
    }
  }

}
