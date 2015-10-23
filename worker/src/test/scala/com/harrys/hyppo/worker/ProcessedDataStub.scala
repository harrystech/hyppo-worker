package com.harrys.hyppo.worker

import com.harrys.hyppo.source.api.{ProcessedDataIntegration, ValidationResult}
import com.harrys.hyppo.source.api.data.AvroRecordType
import com.harrys.hyppo.source.api.model.{DataIngestionTask, IngestionSource, DataIngestionJob}
import com.harrys.hyppo.source.api.task._
import com.harrys.hyppo.worker.rt.data.TestRecord
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

/**
 * Created by jpetty on 7/22/15.
 */
class ProcessedDataStub extends ProcessedDataIntegration[TestRecord] {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  override def avroType(): AvroRecordType[TestRecord] = AvroRecordType.forClass(classOf[TestRecord])

  override def validateSourceConfiguration(source: IngestionSource): ValidationResult = ValidationResult.valid()

  override def validateJobParameters(job: DataIngestionJob): ValidationResult = ValidationResult.valid()

  override def validateTaskArguments(task: DataIngestionTask): ValidationResult = ValidationResult.valid()

  override def newIngestionTaskCreator(): IngestionTaskCreator = new IngestionTaskCreator {
    override def createIngestionTasks(operation: CreateIngestionTasks): Unit = {
      operation.getTaskBuilder.addTask(JavaConversions.mapAsJavaMap(Map[String, AnyRef]()))
    }
  }

  override def newProcessedDataFetcher(): ProcessedDataFetcher[TestRecord] = new ProcessedDataFetcher[TestRecord] {
    override def fetchProcessedData(operation: FetchProcessedData[TestRecord]): Unit = {
      val record = new TestRecord()
      record.setName("Name Value")
      record.setValueOne(1)
      record.setValueTwo(2L)
      operation.append(record)
    }
  }

  override def newProcessedDataPersister(): ProcessedDataPersister[TestRecord] = new ProcessedDataPersister[TestRecord] {
    override def persistProcessedData(operation: PersistProcessedData[TestRecord]): Unit = {
      JavaConversions.asScalaIterator(operation.openReader()).foreach { record =>
        log.info(s"Recieved record: ${ record.toString }")
      }
    }
  }
}
