package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.executor.proto.com.CreateIngestionTasksCommand
import com.harrys.hyppo.executor.proto.res.CreateIngestionTasksResult
import com.harrys.hyppo.worker.{ProcessedDataStub, TestObjects}

import scala.concurrent.duration._

/**
 * Created by jpetty on 7/23/15.
 */
class CreateTasksCommandTest extends ExecutorCommandTest {

  override def integrationClass = classOf[ProcessedDataStub]

  "The create tasks operation" must {
    val testJob = TestObjects.testIngestionJob
    val output  = commander.executeCommand(new CreateIngestionTasksCommand(testJob))
    val resultObject = output.result

    "produce a correct result type" in {
      resultObject shouldBe a [CreateIngestionTasksResult]
    }

    val taskResult = resultObject.asInstanceOf[CreateIngestionTasksResult]

    "successfully produce an integration task result" in {
      taskResult.getJob.getId shouldEqual testJob.getId
      taskResult.getCreatedTasks.size should be > 0
    }

    "then exit cleanly" in {
      commander.sendExitCommandAndWait(Duration(100, MILLISECONDS)) shouldEqual 0
    }
  }
}
