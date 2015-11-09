package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto.{FailureResponse, WorkerInput, WorkerResponse}

/**
  * Created by jpetty on 11/9/15.
  */
trait SpecificWorkResponseHandler[I <: WorkerInput, R <: WorkerResponse] {

  def handleWorkCompleted(response: R) : Unit

  def handleWorkExpired(expired: I) : Unit

  def handleWorkFailed(input: I, response: FailureResponse) : Unit

}
