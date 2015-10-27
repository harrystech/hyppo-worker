package com.harrys.hyppo.worker.proc

import com.harrys.hyppo.executor.proto.ExecutorError
import com.harrys.hyppo.worker.api.proto.{IntegrationException, IntegrationStackFrame}

import scala.collection.JavaConversions

/**
 * This class represents an exception thrown inside of the executor that has now propagated back to the worker
 */
final class ExecutorException(val executorError: ExecutorError) extends Exception(executorError.getMessage) {

  def toIntegrationException: IntegrationException = {
    ExecutorException.createIntegrationException(executorError)
  }
}

object ExecutorException {

  def createIntegrationException(error: ExecutorError) : IntegrationException = {
    val nextCause  = Option(error.getCause).map(createIntegrationException)
    val stackTrace = JavaConversions.asScalaBuffer(error.getStackTrace).map(e => {
      IntegrationStackFrame(e.getClassName, e.getMethodName, e.getFileName, e.getLineNumber)
    })
    IntegrationException(error.getExceptionType, error.getMessage, stackTrace, nextCause)
  }
}
