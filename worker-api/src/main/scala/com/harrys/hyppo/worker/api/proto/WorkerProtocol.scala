package com.harrys.hyppo.worker.api.proto

import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.source.api.model.{DataIngestionJob, DataIngestionTask, IngestionSource}
import com.harrys.hyppo.worker.api.code.{ExecutableIntegration, IntegrationCode, IntegrationSchema, UnvalidatedIntegration}

/**
 * Created by jpetty on 9/18/15.
 */
sealed trait WorkerInput extends Product with Serializable {
  def code: IntegrationCode
  def source: IngestionSource
}
sealed trait IntegrationWorkerInput extends WorkerInput {
  def integration: ExecutableIntegration
  def job: DataIngestionJob
  override final def code: IntegrationCode   = integration.code
  override final def source: IngestionSource = integration.source
}
sealed trait GeneralWorkerInput extends WorkerInput {
  def integration: UnvalidatedIntegration
  override final def code: IntegrationCode   = integration.code
  override final def source: IngestionSource = integration.source
}

sealed trait WorkerResponse extends Product with Serializable {
  def input: WorkerInput
  def logFile: RemoteLogFile
}

@SerialVersionUID(1L)
final case class FailureResponse
(
  override val input: WorkerInput,
  override val logFile: RemoteLogFile,
  exception: Option[IntegrationException]
) extends WorkerResponse

//
//
//

@SerialVersionUID(1L)
final case class ValidateIntegrationRequest(override val integration: UnvalidatedIntegration) extends GeneralWorkerInput

@SerialVersionUID(1L)
final case class ValidationErrorDetails(message: String, exception: Option[IntegrationException])

@SerialVersionUID(1L)
final case class ValidateIntegrationResponse
(
  override val input: ValidateIntegrationRequest,
  override val logFile: RemoteLogFile,
  isValid: Boolean,
  schema: IntegrationSchema,
  rawDataIntegration: Boolean,
  persistingSemantics: PersistingSemantics,
  validationErrors: Seq[ValidationErrorDetails]
) extends WorkerResponse


//
//
//

@SerialVersionUID(1L)
final case class CreateIngestionTasksRequest
(
  override val integration: ExecutableIntegration,
  job: DataIngestionJob
) extends IntegrationWorkerInput

@SerialVersionUID(1L)
final case class CreateIngestionTasksResponse
(
  override val input: CreateIngestionTasksRequest,
  override val logFile: RemoteLogFile,
  tasks: Seq[DataIngestionTask]
) extends WorkerResponse {

  def job: DataIngestionJob = input.job
}

//
//
//

@SerialVersionUID(1L)
final case class FetchProcessedDataRequest
(
  override val integration: ExecutableIntegration,
  task: DataIngestionTask
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
}


@SerialVersionUID(1L)
final case class FetchProcessedDataResponse
(
  override val input: FetchProcessedDataRequest,
  override val logFile: RemoteLogFile,
  data: RemoteProcessedDataFile
) extends WorkerResponse {

  def task: DataIngestionTask = input.task

}


//
//
//

@SerialVersionUID(1L)
final case class FetchRawDataRequest
(
  override val integration: ExecutableIntegration,
  task: DataIngestionTask
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
}

@SerialVersionUID(1L)
final case class FetchRawDataResponse
(
  override val input: FetchRawDataRequest,
  override val logFile: RemoteLogFile,
  data: Seq[RemoteRawDataFile]
) extends WorkerResponse {

  def task: DataIngestionTask = input.task

}

//
//
//
@SerialVersionUID(1L)
final case class ProcessRawDataRequest
(
  override val integration: ExecutableIntegration,
  task: DataIngestionTask,
  files: Seq[RemoteRawDataFile]
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
}

@SerialVersionUID(1L)
final case class ProcessRawDataResponse
(
  override val input: ProcessRawDataRequest,
  override val logFile: RemoteLogFile,
  data: RemoteProcessedDataFile
) extends WorkerResponse {

  def task: DataIngestionTask = input.task

}


//
//
//

@SerialVersionUID(1L)
final case class ProcessedTaskData(task: DataIngestionTask, file: RemoteProcessedDataFile) extends Serializable

@SerialVersionUID(1L)
final case class PersistProcessedDataRequest
(
  override val integration: ExecutableIntegration,
  override val job:   DataIngestionJob,
  data:  Seq[ProcessedTaskData]
) extends IntegrationWorkerInput

@SerialVersionUID(1L)
final case class PersistProcessedDataResponse
(
  override val input: PersistProcessedDataRequest,
  override val logFile: RemoteLogFile,
  persisted: ProcessedTaskData
) extends WorkerResponse

