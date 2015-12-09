package com.harrys.hyppo.worker.data

import java.io.File
import java.util

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3Object, PutObjectResult}
import com.amazonaws.util.Base64
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.worker.api.code.IntegrationUtils
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

  def remoteLogLocation(input: WorkerInput): RemoteStorageLocation = {
    RemoteStorageLocation(config.dataBucketName, remoteLogKey(input))
  }

  def uploadLogFile(input: WorkerInput, log: File): Future[RemoteLogFile] = Future {
    val location = remoteLogLocation(input)
    val fileSize = FileUtils.sizeOf(log)
    val result   = client.putObject(location.bucket, location.key, log)
    RemoteLogFile(location, fileSize, fingerprintValue(result, log))
  }

  def download(remote: RemoteDataFile): Future[File] = Future {
    val location   = remote.location
    blocking {
      val s3Object = client.getObject(location.bucket, location.key)
      assertChecksumMatch(s3Object, remote)
      val stream   = s3Object.getObjectContent
      try {
        val local  = files.newFile(FilenameUtils.getBaseName(location.key), FilenameUtils.getExtension(location.key))
        FileUtils.copyInputStreamToFile(stream, local)
        local
      } finally {
        IOUtils.closeQuietly(stream)
      }
    }
  }

  def uploadRawData(task: DataIngestionTask, files: Seq[File]) : Future[Seq[RemoteRawDataFile]] = {
    val filePairs = remoteRawDataFileLocations(task, files)
    val futures   = filePairs.map(pair => Future {
      val location  = pair._1
      val localFile = pair._2
      blocking {
        val fileSize = FileUtils.sizeOf(localFile)
        val response = client.putObject(location.bucket, location.key, localFile)
        RemoteRawDataFile(location, fileSize, fingerprintValue(response, localFile))
      }
    })
    Future.sequence(futures)
  }

  def uploadProcessedData(task: DataIngestionTask, file: File, records: Long) : Future[RemoteProcessedDataFile] = Future {
    val location = remoteProcessedDataFileLocation(task, file)
    blocking {
      val fileSize = FileUtils.sizeOf(file)
      val response = client.putObject(location.bucket, location.key, file)
      RemoteProcessedDataFile(location, fileSize, fingerprintValue(response, file), records)
    }
  }

  private def remoteRawDataFileLocations(task: DataIngestionTask, files: Seq[File]) : Seq[(RemoteStorageLocation, File)] = {
    val rawFileRoot = rawDataFileRoot(task)
    files.zipWithIndex.map(fileWithIndex => {
      val file        = fileWithIndex._1
      val index       = fileWithIndex._2
      val specificKey = Seq(rawFileRoot, s"data-$index.raw.gz").mkString("/")
      (RemoteStorageLocation(config.dataBucketName, specificKey), file)
    })
  }

  private def remoteProcessedDataFileLocation(task: DataIngestionTask, file: File): RemoteStorageLocation = {
    val specificKey = Seq(processedDataFileRoot(task), "data.avro").mkString("/")
    RemoteStorageLocation(config.dataBucketName, specificKey)
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

  private def remoteLogKey(input: WorkerInput) : String = {
    val date   = LocalDate.now(DateTimeZone.UTC).toString(LocalDateFormat)
    val prefix = s"${config.storagePrefix}/${input.source.getName}/$date"
    input match {
      case g: GeneralWorkerInput =>
        s"$prefix/validate-${g.integration.version}/log/${ input.executionId.toString }.out"
      case i: IntegrationWorkerInput =>
        s"$prefix/ingestion-job-${i.job.getId.toString}/log/${ input.executionId.toString }.out"
    }
  }

  private def assertChecksumMatch(s3Object: S3Object, remote: RemoteDataFile): Unit = {
    Option(s3Object.getObjectMetadata.getContentMD5).foreach { md5 =>
      if (!java.util.Arrays.equals(Base64.decode(md5), remote.checkSum)){
        throw new IllegalStateException(s"${remote.toString} file checksum didn't match S3 checksum. Expected: ${ Base64.encodeAsString(remote.checkSum:_*) }, found: $md5")
      }
    }
  }

  private def fingerprintValue(response: PutObjectResult, localFile: File): Array[Byte] = {
    Option(response.getContentMd5).map(Base64.decode).getOrElse(IntegrationUtils.computeFileFingerprint(localFile))
  }
}
