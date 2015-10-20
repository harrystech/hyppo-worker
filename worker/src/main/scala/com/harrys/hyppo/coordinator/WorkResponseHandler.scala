package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto._

/**
 * Created by jpetty on 9/16/15.
 */
trait WorkResponseHandler {

  def onIntegrationValidated(validated: ValidateIntegrationResponse) : Unit

  def onIngestionTasksCreated(created: CreateIngestionTasksResponse) : Unit

  def onRawDataFetched(fetched: FetchRawDataResponse) : Unit

  def onRawDataProcessed(processed: ProcessRawDataResponse) : Unit

  def onProcessedDataFetched(fetched: FetchProcessedDataResponse) : Unit

  def onProcessedDataPersisted(persisted: PersistProcessedDataResponse) : Unit

  def onWorkFailed(failure: FailureResponse) : Unit

}
