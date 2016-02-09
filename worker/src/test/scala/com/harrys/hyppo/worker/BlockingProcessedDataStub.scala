package com.harrys.hyppo.worker

import java.time.Duration

import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.source.api.task._
import com.harrys.hyppo.worker.rt.data.TestRecord
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

/**
  * This class represents a simple [[com.harrys.hyppo.source.api.ProcessedDataIntegration]] that
  * blocks for 10 minutes at the beginning of each phase in order to simulate some long-running
  * process
  */
class BlockingProcessedDataStub extends ProcessedDataStub {
  val sleepDuration = Duration.ofMinutes(10)

  override def newIngestionTaskCreator(): IngestionTaskCreator = new IngestionTaskCreator {
    override def createIngestionTasks(operation: CreateIngestionTasks): Unit = {
      Thread.sleep(sleepDuration.toMillis)
      operation.getTaskBuilder.addTask(JavaConversions.mapAsJavaMap(Map[String, AnyRef]()))
    }
  }

  override def newProcessedDataFetcher(): ProcessedDataFetcher[TestRecord] = {
    val parent = super.newProcessedDataFetcher()
    new ProcessedDataFetcher[TestRecord] {
      override def fetchProcessedData(operation: FetchProcessedData[TestRecord]): Unit = {
        Thread.sleep(sleepDuration.toMillis)
        parent.fetchProcessedData(operation)
      }
    }
  }

  override def newProcessedDataPersister(): ProcessedDataPersister[TestRecord] = {
    val parent = super.newProcessedDataPersister()
    new ProcessedDataPersister[TestRecord] {

      override def semantics = PersistingSemantics.Unsafe

      override def persistProcessedData(operation: PersistProcessedData[TestRecord]): Unit = {
        Thread.sleep(sleepDuration.toMillis)
        parent.persistProcessedData(operation)
      }
    }
  }
}
