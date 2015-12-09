package com.harrys.hyppo.worker.proc

import java.io.File
import java.net.ServerSocket

import com.harrys.hyppo.executor.net.WorkerIPCSocket
import com.harrys.hyppo.executor.proto.com.ExitCommand
import com.harrys.hyppo.executor.proto.{OperationResult, StartOperationCommand, StatusUpdate}
import com.harrys.hyppo.worker.exec.TaskLogStrategy
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Created by jpetty on 7/22/15.
 */
final class SimpleCommander(val executor: LaunchedExecutor, server: ServerSocket) {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  def executeCommand(command: StartOperationCommand) : CommandOutput = {
    val updater = (_: StatusUpdate) => {}
    executeCommand(command, updater)
  }

  @throws[CommandExecutionException]("if the task fails to run successfully")
  def executeCommand(command: StartOperationCommand, update: (StatusUpdate) => Unit) : CommandOutput = {
    val handler = new OperationHandler(this.waitForNextConnection())
    try {
      handler.sendCommand(command)
      val result  = consumeForResult(handler, update)
      CommandOutput(result, latestExecutorLogOption())
    } catch {
      case e: ExecutorException =>
        throw new CommandExecutionException(command, e.toIntegrationException, latestExecutorLogOption())
    } finally {
      handler.close()
    }
  }

  @tailrec
  private def consumeForResult(handler: OperationHandler, update: (StatusUpdate) => Unit) : OperationResult = {
    handler.readStatusUpdateOrResult() match {
      case Right(result) => result
      case Left(status)  =>
        update(status)
        consumeForResult(handler, update)
    }
  }

  def sendExitCommand() : Unit = {
    log.debug("Sending executor the exit command")
    val handler = new OperationHandler(this.waitForNextConnection())
    try {
      handler.sendCommand(new ExitCommand())
    } finally {
      handler.close()
    }
  }

  def sendExitCommandAndWait(wait: FiniteDuration) : Int = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val deadline = Deadline.now + wait
    val future   = Future({
      sendExitCommand()
      executor.waitForExit(deadline.timeLeft)
    })
    Await.result(future, wait)
    forceShutdown()
  }

  def sendExitCommandAndWaitThenKill(killTimeout: FiniteDuration) : Int = {
    val deadline = Deadline.now + killTimeout
    new Thread(exitTimeoutRunnable(deadline)).start()
    sendExitCommandAndWait(deadline.timeLeft)
  }

  def isAlive : Boolean  = this.executor.isAlive

  def isClosed : Boolean = this.server.isClosed

  def forceShutdown() : Int = {
    try {
      executor.killProcess()
    } finally {
      IOUtils.closeQuietly(server)
    }
  }

  private def latestExecutorLogOption(): Option[File] = {
    if (executor.logStrategy == TaskLogStrategy.FileTaskLogStrategy){
      Some(executor.files.lastStdoutFile)
    } else {
      None
    }
  }

  private def waitForNextConnection() : WorkerIPCSocket = {
    blocking {
      WorkerIPCSocket.forClientSocket(server.accept())
    }
  }


  private def exitTimeoutRunnable(deadline: Deadline) : Runnable = new Runnable {
    override def run(): Unit = {
      try {
        while (executor.isAlive && deadline.hasTimeLeft()){
          if (deadline.timeLeft.toMillis <= 50L){
            Thread.`yield`()
          } else {
            Thread.sleep(50L)
          }
        }
      } finally {
        if (executor.isAlive){
          log.error("Forcefully stopping executor due to timeout during shutdown sequence")
        }
        forceShutdown()
      }
    }
  }
}
