package com.harrys.hyppo.worker.proc

import com.harrys.hyppo.executor.proto.ExecutorError
import com.harrys.hyppo.worker.api.proto.RemoteException

import scala.collection.JavaConversions

/**
 * Created by jpetty on 9/9/15.
 */
final class ExecutorException(val executorError: ExecutorError) extends Exception {

  def toRemoteException: RemoteException = {
    def internalRemoteMapping(error: ExecutorError) : RemoteException = {
      val cause = Option(error.getCause).map(internalRemoteMapping)
      val trace = JavaConversions.asScalaBuffer(error.getStackTrace).map(_.toStackTraceElement.toString)
      RemoteException(error.getExceptionType, error.getMessage, trace, cause)
    }
    internalRemoteMapping(executorError)
  }
}
