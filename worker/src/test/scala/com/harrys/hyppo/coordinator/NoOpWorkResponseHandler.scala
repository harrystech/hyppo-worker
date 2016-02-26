package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto.{FailureResponse, WorkerInput, WorkerResponse}

/**
  * Created by jpetty on 2/26/16.
  */
class NoOpWorkResponseHandler extends WorkResponseHandler {
  override def handleWorkCompleted(response: WorkerResponse): Unit = ()
  override def handleWorkFailed(failed: FailureResponse): Unit = ()
  override def handleWorkExpired(expired: WorkerInput): Unit = ()
}
