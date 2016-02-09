package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.RabbitMQTests

/**
  * Created by jpetty on 2/9/16.
  */
class ResourceTests extends RabbitMQTests("ResourceTests", TestConfig.workerWithRandomQueuePrefix()) {

  ""
}
