package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto._

/**
 * Created by jpetty on 9/16/15.
 */
trait WorkResponseHandler {

  def handleWorkCompleted(response: WorkerResponse) : Unit

  def handleWorkExpired(expired: WorkerInput) : Unit

  def handleWorkFailed(failed: FailureResponse) : Unit

}
