package com.harrys.hyppo.worker.data

import java.io.File
import java.nio.file.Path

import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.worker.api.proto._

import scala.concurrent.Future

/**
  * Created by jpetty on 2/9/16.
  */
trait DataFileHandler {

  def download(remote: RemoteDataFile, tempDirectory: Path): Future[File]

  def uploadRawData(task: DataIngestionTask, files: Seq[File]) : Future[Seq[RemoteRawDataFile]]

  def uploadProcessedData(task: DataIngestionTask, file: File, records: Long) : Future[RemoteProcessedDataFile]

  def uploadLogFile(input: WorkerInput, log: File): Future[RemoteLogFile]

  def remoteLogLocation(input: WorkerInput): RemoteStorageLocation

}
