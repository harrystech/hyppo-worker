package com.harrys.hyppo.worker.actor.data

import java.io.File
import java.nio.file.Files
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.worker.api.code.IntegrationUtils
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.data.DataFileHandler
import org.apache.commons.io.FileUtils

import scala.concurrent._

/**
  * Created by jpetty on 2/9/16.
  */
class LocalDataFileHandler @Inject() (implicit @Assisted ec: ExecutionContext) extends DataFileHandler {

  val tempDirectory = Files.createTempDirectory("test")
  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable(){
    override def run(): Unit = {
      FileUtils.deleteDirectory(tempDirectory.toFile)
    }
  }))


  override def download(remote: RemoteDataFile): Future[File] = {
    Future.successful(createLocalFileMapping(remote.location))
  }

  override def uploadProcessedData(task: DataIngestionTask, file: File, records: Long): Future[RemoteProcessedDataFile] = {
    val location    = remoteProcessedDataFileLocation(task, file)
    val fileSize    = FileUtils.sizeOf(file)
    val fingerprint = IntegrationUtils.computeFileFingerprint(file)
    Future.successful(RemoteProcessedDataFile(location, fileSize, fingerprint, records))
  }

  override def uploadRawData(task: DataIngestionTask, files: Seq[File]): Future[Seq[RemoteRawDataFile]] = {
    val filePairs = remoteRawDataFileLocations(task, files)
    val remotes   = filePairs.map(pair => {
      val location    = pair._1
      val localFile   = pair._2
      val fileSize    = FileUtils.sizeOf(localFile)
      val fingerprint = IntegrationUtils.computeFileFingerprint(localFile)
      RemoteRawDataFile(location, fileSize, fingerprint)
    })
    Future.successful(remotes)
  }

  override def uploadLogFile(input: WorkerInput, log: File): Future[RemoteLogFile] = {
    val location = remoteLogLocation(input)
    Future.successful(RemoteLogFile(location, FileUtils.sizeOf(log), IntegrationUtils.computeFileFingerprint(log)))
  }

  override def remoteLogLocation(input: WorkerInput): RemoteStorageLocation = {
    RemoteStorageLocation("LOCAL TEST", remoteLogKey(input))
  }

  private def remoteRawDataFileLocations(task: DataIngestionTask, files: Seq[File]): Seq[(RemoteStorageLocation, File)] = {
    val rawFileRoot = rawDataFileRoot(task)
    files.zipWithIndex.map(fileWithIndex => {
      val file        = fileWithIndex._1
      val index       = fileWithIndex._2
      val specificKey = Seq(rawFileRoot, s"data-$index.raw.gz").mkString("-")
      (RemoteStorageLocation("LOCAL TEST", specificKey), file)
    })
  }

  private def remoteProcessedDataFileLocation(task: DataIngestionTask, file: File): RemoteStorageLocation = {
    val specificKey = Seq(processedDataFileRoot(task), "data.avro").mkString("-")
    RemoteStorageLocation("LOCAL TEST", specificKey)
  }

  private def rawDataFileRoot(task: DataIngestionTask): String = {
    val job    = task.getIngestionJob
    val source = job.getIngestionSource
    s"${source.getName}-job-${job.getId.toString}-raw-task-${task.getTaskNumber}"
  }

  private def processedDataFileRoot(task: DataIngestionTask): String = {
    val job    = task.getIngestionJob
    val source = job.getIngestionSource
    s"${source.getName}-job-${job.getId.toString}-records-task-${task.getTaskNumber}"
  }

  private def remoteLogKey(input: WorkerInput): String = {
    input match {
      case g: GeneralWorkerInput =>
        s"${input.source.getName}-validate-${g.integration.version}-${ input.executionId.toString }.out"
      case i: IntegrationWorkerInput =>
        s"${input.source.getName}-ingestion-job-${i.job.getId.toString}-${ input.executionId.toString }.out"
    }
  }

  private def createLocalFileMapping(location: RemoteStorageLocation): File = {
    val sourceFile = new File(location.key)
    val copiedFile =
      if (sourceFile.isFile){
        val tempFile = tempDirectory.resolve(sourceFile.getName).toFile
        FileUtils.copyFile(sourceFile, tempFile)
        tempFile
      } else if (sourceFile.isDirectory) {
        val tempDir = Files.createDirectory(tempDirectory.resolve(sourceFile.getName)).toFile
        FileUtils.copyDirectory(sourceFile, tempDir)
        tempDir
      } else {
        throw new IllegalArgumentException(s"File at ${ sourceFile.getAbsolutePath } does not exist!")
      }
    copiedFile
  }
}
