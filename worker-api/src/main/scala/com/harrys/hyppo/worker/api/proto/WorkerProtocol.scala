package com.harrys.hyppo.worker.api.proto

import java.time.LocalDateTime
import java.util.UUID

import com.harrys.hyppo.source.api.{LogicalOperation, PersistingSemantics}
import com.harrys.hyppo.source.api.model.{DataIngestionJob, DataIngestionTask, IngestionSource}
import com.harrys.hyppo.worker.api.code.{ExecutableIntegration, IntegrationCode, IntegrationSchema, UnvalidatedIntegration}

/**
 * Created by jpetty on 9/18/15.
 */
sealed trait WorkerInput extends Product with Serializable { self =>
  def code: IntegrationCode
  def source: IngestionSource
  def executionId: UUID
  def resources: Seq[WorkResource]
  def logicalOperation: LogicalOperation
  def summaryString: String = {
    s"${self.productPrefix}(source=${source.getName})"
  }
}
sealed trait IntegrationWorkerInput extends WorkerInput { self =>
  def integration: ExecutableIntegration
  def job: DataIngestionJob
  override final def code: IntegrationCode   = integration.code
  override final def source: IngestionSource = integration.source
  override def summaryString: String = {
    s"${self.productPrefix}(source=${source.getName} job=${job.getId.toString})"
  }
}
sealed trait GeneralWorkerInput extends WorkerInput {
  def integration: UnvalidatedIntegration
  override final def code: IntegrationCode   = integration.code
  override final def source: IngestionSource = integration.source
  /**
    * @return Always an empty sequence of resources for [[GeneralWorkerInput]]. Implementing the method at all
    *         is just a courtesy to classes that only use [[WorkerInput]] instances.
    */
  override final def resources: Seq[WorkResource] = Seq()
}

sealed trait WorkerResponse extends Product with Serializable {
  def input: WorkerInput
  def logFile: RemoteLogFile
  final def executionId: UUID = input.executionId
  final def logicalOperation: LogicalOperation = input.logicalOperation
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
final case class ValidateIntegrationRequest
(
  override val integration: UnvalidatedIntegration,
  override val executionId: UUID
) extends GeneralWorkerInput {
  override def logicalOperation: LogicalOperation = LogicalOperation.ValidateIntegration
}

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
  override val executionId: UUID,
  override val resources: Seq[WorkResource],
  job: DataIngestionJob
) extends IntegrationWorkerInput {
  override def logicalOperation: LogicalOperation = LogicalOperation.CreateIngestionTasks
}

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
  override val executionId: UUID,
  override val resources: Seq[WorkResource],
  task: DataIngestionTask
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
  override def logicalOperation: LogicalOperation = LogicalOperation.FetchProcessedData
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
  override val executionId: UUID,
  override val resources: Seq[WorkResource],
  task: DataIngestionTask
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
  override def logicalOperation: LogicalOperation = LogicalOperation.FetchRawData
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
  override val executionId: UUID,
  override val resources: Seq[WorkResource],
  task: DataIngestionTask,
  files: Seq[RemoteRawDataFile]
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
  override def logicalOperation: LogicalOperation = LogicalOperation.ProcessRawData
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
  override val executionId: UUID,
  override val resources: Seq[WorkResource],
  task:   DataIngestionTask,
  data:   RemoteProcessedDataFile
) extends IntegrationWorkerInput {
  override def job: DataIngestionJob = task.getIngestionJob
  override def logicalOperation: LogicalOperation = LogicalOperation.PersistProcessedData
}

@SerialVersionUID(1L)
final case class PersistProcessedDataResponse
(
  override val input:   PersistProcessedDataRequest,
  override val logFile: RemoteLogFile
) extends WorkerResponse


//
//
//

@SerialVersionUID(1L)
final case class HandleJobCompletedRequest
(
  override val  integration: ExecutableIntegration,
  override val  executionId: UUID,
  override val  resources: Seq[WorkResource],
  completedAt:  LocalDateTime,
  job:          DataIngestionJob,
  tasks:        Seq[DataIngestionTask]
) extends IntegrationWorkerInput {
  override def logicalOperation: LogicalOperation = LogicalOperation.HandleJobCompleted
}

@SerialVersionUID(1L)
final case class HandleJobCompletedResponse
(
  override val input: HandleJobCompletedRequest,
  override val logFile: RemoteLogFile
) extends WorkerResponse

