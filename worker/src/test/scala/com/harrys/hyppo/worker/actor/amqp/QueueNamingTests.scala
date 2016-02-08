package com.harrys.hyppo.worker.actor.amqp

import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.{TestConfig, TestObjects}
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by jpetty on 11/9/15.
  */
class QueueNamingTests extends WordSpecLike with Matchers {

  val config = new WorkerConfig(TestConfig.basicTestConfig)
  val naming = new QueueNaming(config)

  "QueueNaming" must {
    "correctly identify logical groups of integration queue names" in {
      val actualSize    = 10
      val integrations  = randomIntegrationSources(actualSize)
      val concurrency   = naming.concurrencyResource("example resource", 1)
      val queueDetails  = integrations.flatMap(i => {
        Seq(
          SingleQueueDetails(naming.integrationWorkQueueName(i, Seq()), size = 0, rate = 0.0, ready = 0, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime()),
          SingleQueueDetails(naming.integrationWorkQueueName(i, Seq(concurrency)), size = 0, rate = 0.0, ready = 0, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime())
        )
      })
      val grouping = naming.toLogicalQueueDetails(queueDetails)
      grouping should have size actualSize

      val flattened = grouping.flatMap {
        case single: SingleQueueDetails =>  Seq(single)
        case multi: MultiQueueDetails   => multi.queues
      }

      flattened should have size (actualSize * 2)
    }
  }


  def randomIntegrationSources(count: Int) : Seq[ExecutableIntegration] = {
    (1 to count).inclusive.map { i =>
      val source = TestObjects.testIngestionSource(name = "fake-integration-" + i)
      TestObjects.testProcessedDataIntegration(source)
    }
  }
}
