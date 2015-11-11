package com.harrys.hyppo.coordinator

import com.harrys.hyppo.worker.api.proto.{FailureResponse, WorkerInput, WorkerResponse}

import scala.annotation.tailrec

/**
  * Created by jpetty on 11/11/15.
  */
final class WorkResponseHandlerChain(handlers: List[WorkResponseHandler]) extends WorkResponseHandler {

  override def handleWorkCompleted(response: WorkerResponse): Unit = handleWorkCompleted(response, handlers)

  override def handleWorkFailed(failed: FailureResponse): Unit = handleWorkFailed(failed, handlers)

  override def handleWorkExpired(expired: WorkerInput): Unit = handleWorkExpired(expired, handlers)

  @tailrec
  private def handleWorkCompleted(response: WorkerResponse, handlers: List[WorkResponseHandler]): Unit = {
    if (handlers.nonEmpty){
      handlers.head.handleWorkCompleted(response)
      handleWorkCompleted(response, handlers.tail)
    }
  }

  @tailrec
  private def handleWorkFailed(failed: FailureResponse, handlers: List[WorkResponseHandler]): Unit = {
    if (handlers.nonEmpty){
      handlers.head.handleWorkFailed(failed)
      handleWorkFailed(failed, handlers.tail)
    }
  }

  @tailrec
  private def handleWorkExpired(expired: WorkerInput, handlers: List[WorkResponseHandler]): Unit = {
    if (handlers.nonEmpty){
      handlers.head.handleWorkExpired(expired)
      handleWorkExpired(expired, handlers.tail)
    }
  }
}
