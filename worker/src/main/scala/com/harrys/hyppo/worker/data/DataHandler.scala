package com.harrys.hyppo.worker.data

import java.io.File
import java.util.UUID

import com.amazonaws.services.s3.AmazonS3Client
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.worker.api.proto._
import org.apache.commons.io.{FileUtils, FilenameUtils, IOUtils}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, LocalDate}

import scala.concurrent._

/**
 * Created by jpetty on 8/4/15.
 */
final class DataHandler(config: WorkerConfig, files: TempFilePool)(implicit val context: ExecutionContext) {

  private val client = new AmazonS3Client(config.awsCredentialsProvider)

  def uploadLogFile(location: RemoteLogFile, logFile: File) : Future[RemoteLogFile] = {
    if (!config.uploadTaskLog){
      Future.successful(location)
    } else {
      Future({
        blocking {
          client.putObject(location.bucket, location.key, logFile)
        }
        location
      })
    }
  }

  def download(remote: RemoteDataFile) : Future[File] = Future {
    blocking {
      val s3Object = client.getObject(remote.bucket, remote.key)
      val stream   = s3Object.getObjectContent
      try {
        val local  = files.newFile(FilenameUtils.getBaseName(remote.key), FilenameUtils.getExtension(remote.key))
        FileUtils.copyInputStreamToFile(stream, local)
        local
      } finally {
        IOUtils.closeQuietly(stream)
      }
    }
  }

  def uploadRawData(task: DataIngestionTask, files: Seq[File]) : Future[Seq[RemoteRawDataFile]] = {
    val filePairs = createRemoteRawDataFiles(task, files)
    val futures   = filePairs.map(pair => Future {
      val remote  = pair._1
      val local   = pair._2
      blocking {
        client.putObject(remote.bucket, remote.key, local)
      }
      remote
    })
    Future.sequence(futures)
  }

  def uploadProcessedData(task: DataIngestionTask, file: File, records: Long) : Future[RemoteProcessedDataFile] = Future {
    val remote = createRemoteProcessedDataFile(task, file, records)
    blocking {
      client.putObject(remote.bucket, remote.key, file)
    }
    remote
  }

  def createRemoteLogFile(input: WorkerInput, file: File) : RemoteLogFile = {
    val specificKey = Seq(outputLogRoot(input), UUID.randomUUID().toString + ".out").mkString("/")
    RemoteLogFile(config.dataBucketName, specificKey)
  }

  private def createRemoteRawDataFiles(task: DataIngestionTask, files: Seq[File]) : Seq[(RemoteRawDataFile, File)] = {
    val rawFileRoot = rawDataFileRoot(task)
    files.zipWithIndex.map(fileWithIndex => {
      val file  = fileWithIndex._1
      val index = fileWithIndex._2
      val specificKey = Seq(rawFileRoot, s"data-${index}.raw.gz").mkString("/")
      (RemoteRawDataFile(config.dataBucketName, specificKey, FileUtils.sizeOf(file)), file)
    })
  }

  private def createRemoteProcessedDataFile(task: DataIngestionTask, file: File, records: Long) : RemoteProcessedDataFile = {
    val specificKey = Seq(processedDataFileRoot(task), "data.avro").mkString("/")
    RemoteProcessedDataFile(config.dataBucketName, specificKey, records)
  }

  private val LocalDateFormat = ISODateTimeFormat.date()

  private def rawDataFileRoot(task: DataIngestionTask) : String = {
    val job    = task.getIngestionJob
    val source = job.getIngestionSource
    val date   = new LocalDate(job.getStartedAt, DateTimeZone.UTC).toString(LocalDateFormat)
    s"${config.storagePrefix}/${source.getName}/$date/job-${job.getId.toString}/raw/task-${task.getTaskNumber}"
  }

  private def processedDataFileRoot(task: DataIngestionTask) : String = {
    val job    = task.getIngestionJob
    val source = job.getIngestionSource
    val date   = new LocalDate(job.getStartedAt, DateTimeZone.UTC).toString(LocalDateFormat)
    s"${config.storagePrefix}/${source.getName}/$date/job-${job.getId.toString}/records/task-${task.getTaskNumber}"
  }

  private def outputLogRoot(input: WorkerInput) : String = {
    val date   = LocalDate.now(DateTimeZone.UTC).toString(LocalDateFormat)
    val prefix = s"${config.storagePrefix}/${input.source.getName}/$date"
    input match {
      case g: GeneralWorkerInput =>
        s"$prefix/validate-${g.integration.version}/log"
      case i: IntegrationWorkerInput =>
        s"$prefix/job-${i.job.getId.toString}/log"
    }
  }
}
