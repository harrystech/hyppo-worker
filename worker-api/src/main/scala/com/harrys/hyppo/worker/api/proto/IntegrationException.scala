package com.harrys.hyppo.worker.api.proto

import java.io.{PrintWriter, StringWriter}

/**
 * Created by jpetty on 10/27/15.
 */
@SerialVersionUID(1L)
final case class IntegrationStackFrame(className: String, methodName: String, fileName: String, lineNumber: Int) {

  /**
   * @return A [[java.lang.StackTraceElement]] that is constructed from the component fields of this instance
   */
  def toStackTraceElement: StackTraceElement = {
    new StackTraceElement(className, methodName, fileName, lineNumber)
  }

  override def toString: String = toStackTraceElement.toString
}

@SerialVersionUID(1L)
final case class IntegrationException(exceptionClass: String, message: String, stackTrace: Seq[IntegrationStackFrame], cause: Option[IntegrationException]) {

  /**
   * @return A summary of the exception, including the original class and the message, but no stack trace
   */
  def summary: String = s"$exceptionClass: $message"

  /**
   * @return Creates a simple [[scala.Exception]] instance with the original exception class prefixed to the message. This
   *         is useful when dealing with exceptions that occur inside of the executor that may have an exception type we
   *         can't explicitly construct without the integration classes
   */
  def toSimpleException: Exception = {
    val causeException = cause.map(_.toSimpleException).orNull
    val simplified     = new Exception(summary, causeException)
    simplified.setStackTrace(stackTrace.map(_.toStackTraceElement).toArray)
    simplified
  }

  /**
   * @return A full stack trace constructed using a the generic [[scala.Exception]] instance created by [[toSimpleException]]
   */
  def toFullTrace: String = {
    val writer = new StringWriter()
    toSimpleException.printStackTrace(new PrintWriter(writer))
    writer.toString
  }
}

object IntegrationException {

  def fromThrowable(t: Throwable) : IntegrationException = {
    val cause = Option(t.getCause).map(fromThrowable)
    val trace = t.getStackTrace.map(e => IntegrationStackFrame(e.getClassName, e.getMethodName, e.getFileName, e.getLineNumber))
    val msg   = Option(t.getMessage).getOrElse("")
    IntegrationException(t.getClass.getCanonicalName, msg, trace, cause)
  }

}
