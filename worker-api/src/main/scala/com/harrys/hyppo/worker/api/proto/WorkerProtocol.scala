package com.harrys.hyppo.worker.api.proto

import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.source.api.model.{DataIngestionTask, DataIngestionJob}
import com.harrys.hyppo.worker.api.code.{IntegrationCode, ExecutableIntegration, IntegrationSchema, UnvalidatedIntegration}

/**
 * Created by jpetty on 9/18/15.
 */
sealed trait WorkerInput extends Product with Serializable {
  def code: IntegrationCode
}
sealed trait IntegrationWorkerInput extends WorkerInput {
  def integration: ExecutableIntegration
  override final def code: IntegrationCode = integration.code
}
sealed trait GeneralWorkerInput extends WorkerInput {
  def integration: UnvalidatedIntegration
  override final def code: IntegrationCode = integration.code
}

sealed trait WorkerResponse extends Product with Serializable {
  def input: WorkerInput
}

@SerialVersionUID(1L)
final case class FailureResponse(override val input: WorkerInput, failure: Option[RemoteException]) extends WorkerResponse

//
//
//

@SerialVersionUID(1L)
final case class ValidateIntegrationRequest(override val integration: UnvalidatedIntegration) extends GeneralWorkerInput

@SerialVersionUID(1L)
final case class ValidationErrorDetails(message: String, exception: Option[RemoteException])

@SerialVersionUID(1L)
final case class ValidateIntegrationResponse
(
  override val input: ValidateIntegrationRequest,
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
  tasks: Seq[DataIngestionTask]
) extends WorkerResponse {

  def job: DataIngestionJob = input.job
}

//
//
//

@SerialVersionUID(1L)
final case class FetchProcessedDataRequest(override val integration: ExecutableIntegration, task: DataIngestionTask) extends IntegrationWorkerInput


@SerialVersionUID(1L)
final case class FetchProcessedDataResponse
(
  override val input: FetchProcessedDataRequest,
  data: RemoteProcessedDataFile
) extends WorkerResponse {

  def task: DataIngestionTask = input.task

}


//
//
//

@SerialVersionUID(1L)
final case class FetchRawDataRequest(override val integration: ExecutableIntegration, task: DataIngestionTask) extends IntegrationWorkerInput

@SerialVersionUID(1L)
final case class FetchRawDataResponse
(
  override val input: FetchRawDataRequest,
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
) extends IntegrationWorkerInput

@SerialVersionUID(1L)
final case class ProcessRawDataResponse
(
  override val input: ProcessRawDataRequest,
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
  job:   DataIngestionJob,
  data:  Seq[ProcessedTaskData]
) extends IntegrationWorkerInput

@SerialVersionUID(1L)
final case class PersistProcessedDataResponse
(
  override val input: PersistProcessedDataRequest,
  persisted: ProcessedTaskData
) extends WorkerResponse

