package com.harrys.hyppo.worker.proc

import com.harrys.hyppo.worker.api.proto.IntegrationException
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.{ClassTag, classTag}

/**
  * Created by jpetty on 12/11/15.
  */
class CommandExceptionTest extends WordSpec with Matchers {

  "The CommandExecutionException" must {
    "produce the original context when possible" in {
      val original    = createStackTrace(new IllegalArgumentException("Testing"))
      val integration = IntegrationException.fromThrowable(original)
      val commandExec = new CommandExecutionException(null, integration, None)
      commandExec.toOriginal shouldBe a[IllegalArgumentException]
    }

    "produce the correct lineage of nesting" in {
      val original = createNested[IllegalArgumentException](new NumberFormatException("Nested"))
      val integration = IntegrationException.fromThrowable(original)
      val commandExec = new CommandExecutionException(null, integration, None)
      commandExec.toOriginal shouldBe a[IllegalArgumentException]
      commandExec.toOriginal.getCause shouldBe a[NumberFormatException]
    }
  }


  def createStackTrace[T <: Exception](b: => T): T = {
    try {
      throw b
    } catch {
      case e: Exception => e.asInstanceOf[T]
    }
  }

  def createNested[T <: Exception : ClassTag](cause: Exception): Exception = {
    try {
      throw cause
    } catch {
      case e: Exception =>
        createStackTrace(classTag[T].runtimeClass.asInstanceOf[Class[Exception]].getConstructor(classOf[String], classOf[Throwable]).newInstance(null, e))
    }
  }
}
