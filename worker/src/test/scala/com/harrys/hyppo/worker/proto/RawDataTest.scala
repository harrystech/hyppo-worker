package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.executor.proto.com.{FetchRawDataCommand, PersistProcessedDataCommand, ProcessRawDataCommand}
import com.harrys.hyppo.executor.proto.res.{FetchRawDataResult, PersistProcessedDataResult, ProcessRawDataResult}
import com.harrys.hyppo.worker.{RawDataStub, TestObjects}

import scala.concurrent.duration._

/**
 * Created by jpetty on 7/23/15.
 */
class RawDataTest extends ExecutorCommandTest {

  override def integrationClass = classOf[RawDataStub]

  "Raw Data Integrations" must {
    val testTask = TestObjects.testIngestionTask()

    val fetchResult = commander.executeCommand(new FetchRawDataCommand(testTask)).result
    "produce a correct result type" in {
      fetchResult shouldBe a [FetchRawDataResult]
    }

    val rawFetch = fetchResult.asInstanceOf[FetchRawDataResult]
    "with sane values for that raw data" in {
      rawFetch.getRawDataFiles shouldNot be(empty)
      rawFetch.getTask.getTaskNumber shouldEqual testTask.getTaskNumber
    }

    val processResult = commander.executeCommand(new ProcessRawDataCommand(testTask, rawFetch.getRawDataFiles)).result
    "then produce the correct processing tyep" in {
      processResult shouldBe a [ProcessRawDataResult]
    }

    val process = processResult.asInstanceOf[ProcessRawDataResult]
    "with sane values for that processed data" in {
      process.getRecordCount shouldEqual 1
      process.getLocalDataFile should exist
    }

    val persistResult = commander.executeCommand(new PersistProcessedDataCommand(testTask, process.getLocalDataFile)).result

    "then produce a correct persisting type" in {
      persistResult shouldBe a [PersistProcessedDataResult]
    }

    val persist = persistResult.asInstanceOf[PersistProcessedDataResult]
    "with sane values for the persisting data" in {
      persist.getTask.getTaskNumber shouldEqual testTask.getTaskNumber
    }

    "exit cleanly" in {
      commander.sendExitCommandAndWait(Duration(1, SECONDS)) shouldEqual 0
    }
  }
}
