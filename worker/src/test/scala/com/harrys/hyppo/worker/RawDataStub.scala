package com.harrys.hyppo.worker

import java.io.ByteArrayInputStream

import com.harrys.hyppo.source.api.{RawDataIntegration, ValidationResult}
import com.harrys.hyppo.source.api.data.AvroRecordType
import com.harrys.hyppo.source.api.model.{DataIngestionTask, IngestionSource, DataIngestionJob}
import com.harrys.hyppo.source.api.task._
import com.harrys.hyppo.worker.rt.data.TestRecord
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.io.Source

/**
 * Created by jpetty on 7/23/15.
 */
class RawDataStub extends RawDataIntegration[TestRecord] {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  override def avroType(): AvroRecordType[TestRecord] = AvroRecordType.forClass(classOf[TestRecord])

  override def validateSourceConfiguration(source: IngestionSource): ValidationResult = ValidationResult.valid()

  override def validateJobParameters(job: DataIngestionJob): ValidationResult = ValidationResult.valid()

  override def validateTaskArguments(task: DataIngestionTask): ValidationResult = ValidationResult.valid()

  override def newRawDataFetcher(): RawDataFetcher = new RawDataFetcher {
    override def fetchRawData(operation: FetchRawData): Unit = {
      operation.addData(new ByteArrayInputStream("hello".getBytes))
    }
  }

  override def newRawDataProcessor(): RawDataProcessor[TestRecord] = new RawDataProcessor[TestRecord] {
    override def processRawData(operation: ProcessRawData[TestRecord]): Unit = {
      val value  = Source.fromInputStream(operation.openInputStream()).mkString
      if (value.equals("hello")){
        val record = TestRecord.newBuilder()
          .setName("Test Name")
          .setValueOne(1)
          .setValueTwo(2L).build()
        operation.append(record)
      }
    }
  }

  override def newProcessedDataPersister(): ProcessedDataPersister[TestRecord] = new ProcessedDataPersister[TestRecord] {
    override def persistProcessedData(operation: PersistProcessedData[TestRecord]): Unit = {
      JavaConversions.asScalaIterator(operation.openReader()).foreach(record => {
        log.info(s"Found record: ${record.getName}")
      })
    }
  }

  override def newIngestionTaskCreator(): IngestionTaskCreator = new IngestionTaskCreator {
    override def createIngestionTasks(operation: CreateIngestionTasks): Unit = {
      operation.createTaskWithArgs(JavaConversions.mapAsJavaMap(Map[String, AnyRef]()))
    }
  }
}
