package com.harrys.hyppo.worker.actor.queue

import java.time.Duration

import com.harrys.hyppo.worker.actor.amqp.QueueNaming
import com.harrys.hyppo.worker.scheduling.RecentResourceContention
import com.harrys.hyppo.worker.{ProcessedDataStub, TestConfig, TestObjects}
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by jpetty on 2/23/16.
  */
class QueueMetricsTrackerTests extends WordSpec with Matchers {

  val config  = TestConfig.workerWithRandomQueuePrefix()
  val naming  = new QueueNaming(config)
  val backoff = Duration.ofMillis(config.resourceBackoffMaxValue.toMillis)

  val integrationOne = TestObjects.testExecutableIntegration(TestObjects.testIngestionSource(name = "One"), new ProcessedDataStub())
  val integrationTwo = TestObjects.testExecutableIntegration(TestObjects.testIngestionSource(name = "Two"), new ProcessedDataStub())

  val concurrency = naming.concurrencyResource("Test Resource", 1)
  val throttling  = naming.throttledResource("Test Throttle", Duration.ofSeconds(10))

  "The DefaultQueueMetricsTracker" must {
    val tracker = new DefaultQueueMetricsTracker(naming, new RecentResourceContention(backoff))

    "register resources to queues when notified about resource acquisition results" in {
      val resources = Seq(concurrency)
      val queueName = naming.integrationWorkQueueName(integrationOne, resources)
      tracker.resourcesAcquiredSuccessfully(queueName, resources)
      val metrics = tracker.integrationQueueMetrics().find(_.details.queueName == queueName)
      metrics.isDefined shouldBe true
      metrics.get.resources.map(_.resource) shouldEqual resources
    }

    "remap resources upon full refresh" in {

    }
  }
}
