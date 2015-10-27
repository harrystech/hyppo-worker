package com.harrys.hyppo.worker.proc

import java.net.ServerSocket

import com.harrys.hyppo.executor.net.WorkerIPCSocket
import com.harrys.hyppo.executor.proto.com.ExitCommand
import com.harrys.hyppo.executor.proto.{OperationResult, StartOperationCommand, StatusUpdate}
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
      val result = consumeForResult(handler, update)
      CommandOutput(result, executor.files.lastStdoutFile)
    } catch {
      case e: ExecutorException =>
        val lastLogFile = executor.files.lastStdoutFile
        throw new CommandExecutionException(command, e.toIntegrationException, lastLogFile)
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

  private def waitForNextConnection() : WorkerIPCSocket = {
    blocking {
      WorkerIPCSocket.forClientSocket(server.accept())
    }
  }


  private def exitTimeoutRunnable(deadline: Deadline) : Runnable = new Runnable {
    override def run(): Unit = {
      try {
        while (executor.isAlive && deadline.hasTimeLeft()){
          Thread.sleep(50L)
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
