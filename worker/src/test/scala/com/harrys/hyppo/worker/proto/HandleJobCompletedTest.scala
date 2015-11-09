package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.executor.proto.com.HandleJobCompletedCommand
import com.harrys.hyppo.executor.proto.res.HandleJobCompletedResult
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.{ProcessedDataStub, TestObjects}

import scala.collection.JavaConversions
import scala.concurrent.duration._

/**
  * Created by jpetty on 11/9/15.
  */
class HandleJobCompletedTest extends ExecutorCommandTest {

  override def integrationClass = classOf[ProcessedDataStub]

  "The job completed command" must {
    val timestamp = TimeUtils.currentLocalDateTime()
    val testTask  = TestObjects.testIngestionTask()
    val taskList  = JavaConversions.seqAsJavaList(Seq(testTask))
    "produce a valid result type" in {
      val result  = commander.executeCommand(new HandleJobCompletedCommand(timestamp, testTask.getIngestionJob, taskList)).result
      result shouldBe a[HandleJobCompletedResult]
    }
    "then exit cleanly" in {
      commander.sendExitCommandAndWait(Duration(100, MILLISECONDS)) shouldEqual 0
    }
  }
}
