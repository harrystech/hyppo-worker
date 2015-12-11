package com.harrys.hyppo.util

import com.harrys.hyppo.worker.api.proto.IntegrationException
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by jpetty on 12/11/15.
  */
object ExceptionUtil {
  private val log = Logger(LoggerFactory.getLogger(this.getClass.getName.replaceAll("\\$", "")))

  def reconstructIntegrationException(error: IntegrationException): Exception = {
    val next = error.cause.map(reconstructIntegrationException)
    val inst = reconstructExceptionInstance(error.exceptionClass, error.message, next)
    inst.setStackTrace(error.stackTrace.map(_.toStackTraceElement).toArray)
    inst
  }

  private def reconstructExceptionInstance(className: String, message: String, cause: Option[Throwable]): Exception = {
    val base  = classOf[Exception]
    val klass = Try(Class.forName(className)) match {
      case Success(c) if base.isAssignableFrom(c) =>
        log.debug(s"Using native exception class ${ c.getName } to reconstitute exception")
        c.asInstanceOf[Class[Exception]]
      case Failure(e) =>
        log.debug(s"Could not find class ${ className } to reconstitute the original exception. Using base Exception class instead")
        base
    }

    var result: Try[Exception] = Try(klass.getConstructor(classOf[String], classOf[Throwable])).map { constructor =>
      log.debug(s"Found original exception constructor with string, throwable arguments for $className")
      constructor.newInstance(exceptionMessage(className, klass, message), cause.orNull)
    }

    if (cause.isEmpty){
      result = result.orElse(Try(klass.getConstructor(classOf[String])).map { constructor =>
        log.debug(s"Found original exception constructor with string argument for $className")
        constructor.newInstance(exceptionMessage(className, klass, message))
      })

      if (StringUtils.isBlank(message)){
        result = result.orElse(Try(klass.newInstance()).map { e =>
          log.debug(s"Found no-arg constructor for class $className")
          e
        })
      }
    }

    result match {
      case Success(e) => e
      case Failure(e) =>
        log.debug(s"No constructor available for $className to use for reconstruction. Falling back to base Exception class", e)
        new Exception(exceptionMessage(className, classOf[Exception], message), cause.orNull)
    }
  }

  private def exceptionMessage(originalClassName: String, found: Class[Exception], message: String): String = {
    if (originalClassName == found.getCanonicalName) {
      message
    } else {
      val base = s"(originally $originalClassName)"
      if (StringUtils.isBlank(message)) base else base + " - " + message
    }
  }
}
