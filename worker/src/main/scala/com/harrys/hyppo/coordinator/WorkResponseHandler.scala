package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto._

/**
 * Created by jpetty on 9/16/15.
 */
trait WorkResponseHandler {

  def validateIntegrationHandler: SpecificWorkResponseHandler[ValidateIntegrationRequest, ValidateIntegrationResponse]

  def ingestionTaskCreationHandler: SpecificWorkResponseHandler[CreateIngestionTasksRequest, CreateIngestionTasksResponse]

  def processedDataFetchingHandler: SpecificWorkResponseHandler[FetchProcessedDataRequest, FetchProcessedDataResponse]

  def rawDataFetchingHandler: SpecificWorkResponseHandler[FetchRawDataRequest, FetchRawDataResponse]

  def rawDataProcessingHandler: SpecificWorkResponseHandler[ProcessRawDataRequest, ProcessRawDataResponse]

  def processedDataPersistingHandler: SpecificWorkResponseHandler[PersistProcessedDataRequest, PersistProcessedDataResponse]

  def onJobCompletedHandler: SpecificWorkResponseHandler[HandleJobCompletedRequest, HandleJobCompletedResponse]


  final def handleWorkCompleted(response: WorkerResponse) : Unit = response match {
    case r: FailureResponse =>
      handleWorkFailed(r)
    case r: ValidateIntegrationResponse =>
      validateIntegrationHandler.handleWorkCompleted(r)
    case r: CreateIngestionTasksResponse =>
      ingestionTaskCreationHandler.handleWorkCompleted(r)
    case r: FetchProcessedDataResponse =>
      processedDataFetchingHandler.handleWorkCompleted(r)
    case r: FetchRawDataResponse =>
      rawDataFetchingHandler.handleWorkCompleted(r)
    case r: ProcessRawDataResponse =>
      rawDataProcessingHandler.handleWorkCompleted(r)
    case r: PersistProcessedDataResponse =>
      processedDataPersistingHandler.handleWorkCompleted(r)
    case r: HandleJobCompletedResponse =>
      onJobCompletedHandler.handleWorkCompleted(r)
  }

  final def handleWorkExpired(expired: WorkerInput) : Unit = expired match {
    case i: ValidateIntegrationRequest  =>
      validateIntegrationHandler.handleWorkExpired(i)
    case i: CreateIngestionTasksRequest =>
      ingestionTaskCreationHandler.handleWorkExpired(i)
    case i: FetchProcessedDataRequest   =>
      processedDataFetchingHandler.handleWorkExpired(i)
    case i: FetchRawDataRequest         =>
      rawDataFetchingHandler.handleWorkExpired(i)
    case i: ProcessRawDataRequest       =>
      rawDataProcessingHandler.handleWorkExpired(i)
    case i: PersistProcessedDataRequest =>
      processedDataPersistingHandler.handleWorkExpired(i)
    case i: HandleJobCompletedRequest   =>
      onJobCompletedHandler.handleWorkExpired(i)
  }

  final def handleWorkFailed(failed: FailureResponse) : Unit = failed.input match {
    case i: ValidateIntegrationRequest  =>
      validateIntegrationHandler.handleWorkFailed(i, failed)
    case i: CreateIngestionTasksRequest =>
      ingestionTaskCreationHandler.handleWorkFailed(i, failed)
    case i: FetchProcessedDataRequest   =>
      processedDataFetchingHandler.handleWorkFailed(i, failed)
    case i: FetchRawDataRequest         =>
      rawDataFetchingHandler.handleWorkFailed(i, failed)
    case i: ProcessRawDataRequest       =>
      rawDataProcessingHandler.handleWorkFailed(i, failed)
    case i: PersistProcessedDataRequest =>
      processedDataPersistingHandler.handleWorkFailed(i, failed)
    case i: HandleJobCompletedRequest   =>
      onJobCompletedHandler.handleWorkFailed(i, failed)
  }
}
