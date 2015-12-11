package com.harrys.hyppo.worker.actor.amqp

import akka.pattern.gracefulStop
import akka.testkit.TestActorRef
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.coordinator.WorkResponseHandler
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
    override def handleWorkCompleted(response: WorkerResponse): Unit = {}
    override def handleWorkFailed(failed: FailureResponse): Unit = {}
    override def handleWorkExpired(expired: WorkerInput): Unit = {}
  }

  override implicit val patienceConfig = PatienceConfig(timeout = config.rabbitMQTimeout * 8, interval = config.rabbitMQTimeout / 4)

  "The WorkResultConsumer" must {
    val consumer = TestActorRef(new ResponseQueueConsumer(config, connectionActor, handler), "consumer")

    "handle the initialize message" in {
      consumer ! Lifecycle.ApplicationStarted
    }

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
