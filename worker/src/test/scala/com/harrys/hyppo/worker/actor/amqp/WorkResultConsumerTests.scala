package com.harrys.hyppo.worker.actor.amqp

import akka.pattern.gracefulStop
import akka.testkit.TestActorRef
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.coordinator.{SpecificWorkResponseHandler, WorkResponseHandler}
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.RabbitMQTests
import com.harrys.hyppo.worker.api.proto._
import org.scalatest.concurrent.Eventually

import scala.concurrent.Await

/**
 * Created by jpetty on 9/17/15.
 */
class WorkResultConsumerTests extends RabbitMQTests("WorkResultConsumerTests", TestConfig.coordinatorWithRandomQueuePrefix()) with Eventually {

  val handler = new WorkResponseHandler {
    override val validateIntegrationHandler = new SpecificWorkResponseHandler[ValidateIntegrationRequest, ValidateIntegrationResponse]{
      override def handleWorkCompleted(response: ValidateIntegrationResponse): Unit = {}
      override def handleWorkFailed(input: ValidateIntegrationRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: ValidateIntegrationRequest): Unit = {}
    }

    override val processedDataPersistingHandler = new SpecificWorkResponseHandler[PersistProcessedDataRequest, PersistProcessedDataResponse] {
      override def handleWorkCompleted(response: PersistProcessedDataResponse): Unit = {}
      override def handleWorkFailed(input: PersistProcessedDataRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: PersistProcessedDataRequest): Unit = {}
    }

    override val ingestionTaskCreationHandler = new SpecificWorkResponseHandler[CreateIngestionTasksRequest, CreateIngestionTasksResponse] {
      override def handleWorkCompleted(response: CreateIngestionTasksResponse): Unit = {}
      override def handleWorkFailed(input: CreateIngestionTasksRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: CreateIngestionTasksRequest): Unit = {}
    }

    override val processedDataFetchingHandler = new SpecificWorkResponseHandler[FetchProcessedDataRequest, FetchProcessedDataResponse] {
      override def handleWorkCompleted(response: FetchProcessedDataResponse): Unit = {}
      override def handleWorkFailed(input: FetchProcessedDataRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: FetchProcessedDataRequest): Unit = {}
    }

    override val onJobCompletedHandler = new SpecificWorkResponseHandler[HandleJobCompletedRequest, HandleJobCompletedResponse] {
      override def handleWorkCompleted(response: HandleJobCompletedResponse): Unit = {}
      override def handleWorkFailed(input: HandleJobCompletedRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: HandleJobCompletedRequest): Unit = {}
    }

    override val rawDataProcessingHandler = new SpecificWorkResponseHandler[ProcessRawDataRequest, ProcessRawDataResponse] {
      override def handleWorkCompleted(response: ProcessRawDataResponse): Unit = {}
      override def handleWorkFailed(input: ProcessRawDataRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: ProcessRawDataRequest): Unit = {}
    }

    override val rawDataFetchingHandler = new SpecificWorkResponseHandler[FetchRawDataRequest, FetchRawDataResponse]  {
      override def handleWorkCompleted(response: FetchRawDataResponse): Unit = {}
      override def handleWorkFailed(input: FetchRawDataRequest, response: FailureResponse): Unit = {}
      override def handleWorkExpired(expired: FetchRawDataRequest): Unit = {}
    }
  }

  override implicit val patienceConfig = PatienceConfig(timeout = config.rabbitMQTimeout * 8, interval = config.rabbitMQTimeout / 4)

  "The WorkResultConsumer" must {
    val consumer = TestActorRef(new ResponseQueueConsumer(config, connectionActor, handler), "consumer")

    "create the expiration and results queue if they don't exist" in {
      eventually {
        helpers.passiveQueueDeclaration(connection, naming.expiredQueueName).map(_.getConsumerCount).getOrElse(0) shouldEqual 1
        helpers.passiveQueueDeclaration(connection, naming.resultsQueueName).map(_.getConsumerCount).getOrElse(0) shouldEqual 1
      }
    }

    "gracefully shutdown when told" in {
      val future = gracefulStop(consumer, config.rabbitMQTimeout, Lifecycle.ImpendingShutdown)
      Await.result(future, config.rabbitMQTimeout) shouldBe true
    }
  }

}
