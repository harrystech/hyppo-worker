package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.executor.proto.com.{FetchProcessedDataCommand, PersistProcessedDataCommand}
import com.harrys.hyppo.executor.proto.res.{FetchProcessedDataResult, PersistProcessedDataResult}
import com.harrys.hyppo.worker.{TestObjects, ProcessedDataStub}

import scala.collection.JavaConversions
import scala.concurrent.duration._

/**
 * Created by jpetty on 7/23/15.
 */
class ProcessedDataTest extends ExecutorCommandTest {

  override def integrationClass = classOf[ProcessedDataStub]

  "Processed Data Integrations must" must {
    val testTask = TestObjects.testIngestionTask

    val fetchObject = commander.executeCommand(new FetchProcessedDataCommand(testTask))

    "produce the correct result fetching type" in {
      fetchObject shouldBe a [FetchProcessedDataResult]
    }

    val fetch = fetchObject.asInstanceOf[FetchProcessedDataResult]

    "with sane result values" in {
      fetch.getLocalDataFile should exist
      fetch.getRecordCount.toInt should be > 0
      fetch.getTask.getTaskNumber shouldEqual testTask.getTaskNumber
    }

    "and real records in the output" in {
      val reader = new ProcessedDataStub().avroType().createFileReader(fetch.getLocalDataFile)
      val values = JavaConversions.asScalaIterator(reader).toSeq
      reader.close()
      values.size should be > 0
      values.head.getName shouldNot be (null)
    }

    val persistObject = commander.executeCommand(new PersistProcessedDataCommand(testTask, fetch.getLocalDataFile))

    "then produce the right persisting type" in {
      persistObject shouldBe a [PersistProcessedDataResult]
    }

    val persist = persistObject.asInstanceOf[PersistProcessedDataResult]

    "with sane values" in {
      persist.getTask.getTaskNumber shouldEqual testTask.getTaskNumber
    }

    "then exit cleanly" in {
      commander.sendExitCommandAndWait(Duration(1, SECONDS)) shouldEqual 0
    }
  }
}
