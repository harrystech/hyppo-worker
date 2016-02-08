package com.harrys.hyppo.worker.proc

import com.harrys.hyppo.executor.net.{IPCMessageFrame, WorkerIPCSocket}
import com.harrys.hyppo.executor.proto.init.{ExecutorReady, InitializationFailed}
import com.typesafe.scalalogging.Logger
import com.harrys.hyppo.executor.proto._
import org.apache.commons.io.IOUtils
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Created by jpetty on 7/23/15.
 */
final class OperationHandler(client: WorkerIPCSocket) {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  //  In order to be compatible with our API, we've gotta use the old jackson object mapper here.
  private val mapper = new ObjectMapper()

  def close() : Unit = {
    IOUtils.closeQuietly(client)
  }

  def sendCommand(command: StartOperationCommand) : Unit = {
    this.waitForInitMessage()
    log.debug(s"Sending command: ${command.getOperationType.getName}")
    val content = mapper.writeValueAsBytes(command)
    client.sendFrame(IPCMessageFrame.createFromContent(content))
  }

  @throws[ExecutorException]("when the executor fails to initialize correctly but manages to send a response")
  private def waitForInitMessage() : ExecutorReady = {
    log.debug("Waiting for executor initialization message")
    val frame = client.readFrame()
    Try(mapper.readValue(frame.getContent, classOf[ExecutorInitMessage])) match {
      case Success(ready: ExecutorReady) =>
        log.debug("Executor ready for operation")
        ready
      case Success(error: InitializationFailed) =>
        log.error("Failure in executor initialization", error.getError)
        this.close()
        throw new ExecutorException(error.getError)
      case Success(unknown) =>
        log.error(s"Received unknown executor initialization message: ${ unknown.toString }")
        this.close()
        throw new Exception(s"Received unknown executor initialization message: ${ unknown.toString }")
      case Failure(error) =>
        log.error(s"Failed to parse the executor's response: ${ new String(frame.getContent) }", error)
        this.close()
        throw new Exception("Failed to parse executor initialization message", error)
    }
  }

  @throws[ExecutorException]("when a fatal error occurs inside of the executor")
  def readStatusUpdateOrResult() : Either[StatusUpdate, OperationResult] = {
    log.debug("Waiting for status update or final result")
    val frame  = client.readFrame()
    Try(mapper.readValue(frame.getContent, classOf[StatusUpdate])) match {
      case Success(update) =>
        log.debug(s"Received status update for operation type: ${update.getOperationType.getName}")
        Left(update)
      case Failure(_) =>
        Right(attemptOperationResult(frame))
    }
  }

  @throws[ExecutorException]("when a fatal error occurs inside of the executor")
  private def attemptOperationResult(frame: IPCMessageFrame) : OperationResult = {
    Try(mapper.readValue(frame.getContent, classOf[OperationResult])) match {
      case Success(result) =>
        log.debug(s"Received final result for operation type: ${result.getOperationType.getName}")
        this.close()
        result
      case Failure(jsonError) =>
        Try(mapper.readValue(frame.getContent, classOf[ExecutorError])) match {
          case Success(execError) =>
            log.debug(s"Executor replied with error - ${execError.getExceptionType}: ${execError.getMessage}")
            this.close()
            throw new ExecutorException(execError)
          case Failure(_) =>
            log.error(s"Failed to determine type of executor message: ${ new String(frame.getContent) }", jsonError)
            throw jsonError
        }
    }
  }

}
