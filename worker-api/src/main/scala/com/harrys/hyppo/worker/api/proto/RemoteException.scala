package com.harrys.hyppo.worker.api.proto

import org.apache.commons.lang3.exception.ExceptionUtils

/**
 * Created by jpetty on 8/27/15.
 */
@SerialVersionUID(1L)
final case class RemoteException
(
  errorClass: String,
  message: String,
  trace: Seq[String],
  cause: Option[RemoteException]
) extends Serializable
{

  def detailString: String = s"$errorClass: $message"

}

object RemoteException {

  def createFromThrowable(t: Throwable) : RemoteException = {
    val cause   = Option(t.getCause).map(t => createFromThrowable(t))
    val trace   = ExceptionUtils.getStackFrames(t).toSeq
    val message = Option(t.getMessage).getOrElse("")
    RemoteException(t.getClass.getCanonicalName, message, trace, cause)
  }

}
