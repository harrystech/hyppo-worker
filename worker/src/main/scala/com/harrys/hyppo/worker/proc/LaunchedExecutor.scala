package com.harrys.hyppo.worker.proc

import java.util.concurrent.Executors

import com.harrys.hyppo.worker.exec.{ExecutorFiles, TaskLogStrategy}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Created by jpetty on 7/22/15.
 */
final class LaunchedExecutor(val process: Process, val files: ExecutorFiles, val logStrategy: TaskLogStrategy) {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  //  Contains the result of the process exiting
  private val exitCodeFuture = {
    implicit val exitContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    val exitFuture = Future[Int]({
      blocking {
        process.waitFor()
      }
      process.exitValue()
    })
    exitFuture.onSuccess({
      //  Normal exit condition
      case (code: Int) if code == 0 =>
        log.info(s"Executor process exited with status code: $code")

      //  Unexpected exit condition
      case (code: Int) =>
        log.warn(s"Executor process exited with status code: $code")
        if (logStrategy == TaskLogStrategy.FileTaskLogStrategy){
          Try(Source.fromFile(files.lastStdoutFile).mkString) match {
            case Success(stdout) => log.error(s"Executor STDOUT:\n$stdout")
            case _ => // NOOP
          }
          Try(Source.fromFile(files.standardErrorFile).mkString) match {
            case Success(stderr) => log.error(s"Executor STDERR:\n$stderr")
            case _ => // NOOP
          }
        }
    })
    exitFuture.onComplete({ _ =>
      //  Ensure the background threads are shutdown after the process exits
      exitContext.shutdown()
    })
    exitFuture
  }

  def isAlive : Boolean = process.isAlive

  def exitCode : Option[Int] = {
    exitCodeFuture.value.map(_.getOrElse(-1))
  }

  def waitForExit(timeout: Duration) : Int = {
    Await.result(exitCodeFuture, timeout)
  }

  def waitForExitThenKill(killTimeout: Duration) : Int = {
    Try(waitForExit(killTimeout)) match {
      case Success(code) => code
      case Failure(_)    =>
        this.killProcess()
    }
  }

  def killProcess() : Int = {
    if (process.isAlive){
      log.info("Killing executor process forcibly")
      process.destroyForcibly().waitFor()
    } else {
      process.exitValue()
    }
  }

  def deleteFiles() : Unit = files.destroyAll()
}
