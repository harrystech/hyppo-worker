package com.harrys.hyppo.worker.data

import java.io.File

import com.harrys.hyppo.source.api.model.DataIngestionTask
import com.harrys.hyppo.worker.api.proto._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by jpetty on 2/9/16.
  */
trait DataFileHandler {

  def download(remote: RemoteDataFile): Future[File]

  def uploadRawData(task: DataIngestionTask, files: Seq[File]) : Future[Seq[RemoteRawDataFile]]

  def uploadProcessedData(task: DataIngestionTask, file: File, records: Long) : Future[RemoteProcessedDataFile]

  def uploadLogFile(input: WorkerInput, log: File): Future[RemoteLogFile]

  def remoteLogLocation(input: WorkerInput): RemoteStorageLocation

}

object DataFileHandler {
  trait Factory {
    def apply(tempFiles: TempFilePool)(implicit ec: ExecutionContext): DataFileHandler
  }
}
