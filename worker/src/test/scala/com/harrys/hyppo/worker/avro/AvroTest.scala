package com.harrys.hyppo.worker.avro

import java.io.File
import java.nio.file.Files

import com.harrys.hyppo.source.api.ProcessedDataIntegration
import com.harrys.hyppo.source.api.data.AvroRecordType
import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.source.api.task.FetchProcessedData
import org.apache.avro.file.CodecFactory
import org.apache.avro.specific.SpecificRecord

import scala.collection.JavaConversions

/**
 * Created by jpetty on 10/30/15.
 */
object AvroTest {


  def createSampleDataFile[T <: SpecificRecord](avroType: AvroRecordType[T], records: Iterable[T]) : File = {
    val file   = newTempTestFile()
    val writer = avroType.createAvroRecordAppender(file, CodecFactory.fromString("deflate"))
    try {
      writer.appendAll(JavaConversions.asJavaIterable(records))
      file
    } finally {
      writer.close()
    }
  }



  def createProcessedDataFile[T <: SpecificRecord](integration: ProcessedDataIntegration[T], task: DataIngestionTask) : File = {
    val appender  = integration.avroType().createAvroRecordAppender(newTempTestFile(), CodecFactory.fromString("deflate"))
    val operation = new FetchProcessedData[T](task, appender)
    try {
      integration.newProcessedDataFetcher().fetchProcessedData(operation)
      appender.getOutputFile
    } finally {
      appender.close()
    }
  }

  private def newTempTestFile(): File = {
    val testFile = Files.createTempFile("avro-test", "avro").toFile
    testFile.deleteOnExit()
    testFile
  }
}
