package com.harrys.hyppo.worker.actor.amqp

import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.worker.TestConfig

/**
 * Created by jpetty on 11/3/15.
 */
class QueueCreationTests extends RabbitMQTests(new CoordinatorConfig(TestConfig.basicTestConfig))  {

  val rabbitMQ = config.rabbitMQConnectionFactory.newConnection()

  override def localTestCleanup() : Unit = {
    rabbitMQ.close()
  }

  "The Queue Test" must {
    "allow queue creation" in {
      helpers.isQueueDefined(rabbitMQ, "does not") shouldEqual false
      helpers.isQueueDefined(rabbitMQ, naming.resultsQueueName) shouldEqual true
    }
  }
}
