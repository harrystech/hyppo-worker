package com.harrys.hyppo.worker.actor.amqp

import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.RabbitMQTests

/**
 * Created by jpetty on 11/3/15.
 */
class QueueCreationTests extends RabbitMQTests("QueueCreationTests", TestConfig.coordinatorWithRandomQueuePrefix())  {

  "The Queue Test" must {
    "allow queue creation" in {
      helpers.isQueueDefined(connection, "does not") shouldEqual false
      withChannel { channel =>
        helpers.createResultsQueue(channel)
        helpers.isQueueDefined(connection, naming.resultsQueueName) shouldEqual true
      }
    }
  }
}
