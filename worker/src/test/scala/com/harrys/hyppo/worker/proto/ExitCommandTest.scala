package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.worker.ProcessedDataStub

import scala.concurrent.duration._

/**
 * Created by jpetty on 7/23/15.
 */
class ExitCommandTest extends ExecutorCommandTest {

  override def integrationClass = classOf[ProcessedDataStub]

  "The exit command" must {
    "cause the executor to exit with status code 0" in {
      commander.sendExitCommandAndWait(Duration(5, SECONDS)) shouldEqual 0
    }
  }
}
