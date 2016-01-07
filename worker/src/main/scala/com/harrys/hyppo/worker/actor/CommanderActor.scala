package com.harrys.hyppo.worker.actor

import java.io.File
import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.executor.cli.ExecutorMain
import com.harrys.hyppo.executor.proto.com._
import com.harrys.hyppo.executor.proto.res._
import com.harrys.hyppo.source.api.model.{DataIngestionJob, TaskAssociations}
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent
import com.harrys.hyppo.worker.api.code.{IntegrationCode, IntegrationSchema}
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.data.{DataHandler, LoadedJarFile, TempFilePool}
import com.harrys.hyppo.worker.proc.{CommandExecutionException, CommandOutput, ExecutorException, SimpleCommander}

import scala.collection.JavaConversions
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Created by jpetty on 10/29/15.
 */
class CommanderActor
(
  config:       WorkerConfig,
  integration:  IntegrationCode,
  jarFiles:     Seq[LoadedJarFile]
) extends Actor with ActorLogging {

  import context.dispatcher

  //  Throws an exception if the code fails to match the jar files
  this.assertIntegrationCodeMatchesJars()

  //  System socket for executor setup
  private val simpleCommander: SimpleCommander = {
    val socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress)
    val setup  = config.newExecutorSetup()
    //  Source API
    setup.addToClasspath(new File(classOf[DataIngestionJob].getProtectionDomain.getCodeSource.getLocation.getFile))
    //  Executor JAR file
    setup.addToClasspath(new File(classOf[ExecutorMain].getProtectionDomain.getCodeSource.getLocation.getFile))
    setup.addToClasspath(jarFiles.map(_.file))
    val executor = setup.launchWithArgs(socket.getLocalPort, integration.integrationClass, config.taskLogStrategy, config.avroFileCodec)
    new SimpleCommander(executor, socket)
  }

  val tempFiles   = new TempFilePool(simpleCommander.executor.files.workingDirectory.toPath)
  val dataHandler = new DataHandler(config, tempFiles)

  //  Easier access to executor STDOUT / STDERR streams on disk
  def standardErrorContents: String = Source.fromFile(simpleCommander.executor.files.standardErrorFile).mkString
  def previousOutContents: String = {
    Source.fromFile(simpleCommander.executor.files.lastStdoutFile).mkString
  }

  override def postStop() : Unit = {
    Try(simpleCommander.sendExitCommandAndWaitThenKill(Duration(2, SECONDS)))
    Try(simpleCommander.executor.deleteFiles())
  }

  private final case object WorkCompletedMessage


  private var isRunningWork = false

  override def receive: Receive = {
    case WorkCompletedMessage =>
      if (!isRunningWork){
        log.error("Received {} when not running work", WorkCompletedMessage.productPrefix)
        self ! Failure(new IllegalStateException(s"Commander should never receive ${WorkCompletedMessage.productPrefix} when not running work"))
      } else {
        isRunningWork = false
        log.debug("Commander is now available")
      }

    case Failure(e) =>
      log.error("Terminating due to previous failure")
      context.stop(self)

    case input: WorkerInput =>
      if (isRunningWork){
        log.error("Received work while still running another task. Work received: {}", input.summaryString)
        self ! Failure(new IllegalStateException("Commander received work while still running another task. Work received: " + input.summaryString))
      } else {
        val taskActor  = sender()
        log.info("Starting work on {}", input.summaryString)
        isRunningWork  = true

        executeWorkRequest(taskActor, input).onComplete {
          case Success(_) =>
            log.info("Successfully completed task {}", input.summaryString)
            self ! WorkCompletedMessage
          case Failure(e: CommandExecutionException) =>
            log.debug("Failure already handled at task actor level. Restarting.")
            self ! Failure(e)
          case Failure(e) =>
            log.error(e, "Failed to process task {}", input.summaryString)
            taskActor ! TaskFSMEvent.OperationResponseAvailable(FailureResponse(input, Some(dataHandler.remoteLogLocation(input)), Some(IntegrationException.fromThrowable(e))))
            taskActor ! TaskFSMEvent.OperationLogUploaded
            self ! Failure(e)
        }
      }
  }


  def executeWorkRequest(taskActor: ActorRef, input: WorkerInput) : Future[Unit] = input match {
    case validate: ValidateIntegrationRequest =>
      performIntegrationValidation(taskActor, validate)

    case create: CreateIngestionTasksRequest =>
      performIngestionTaskCreation(taskActor, create)

    case fetch: FetchProcessedDataRequest =>
      performProcessedDataFetching(taskActor, fetch)

    case fetch: FetchRawDataRequest =>
      performRawDataFetching(taskActor, fetch)

    case process: ProcessRawDataRequest =>
      performRawDataProcessing(taskActor, process)

    case persist: PersistProcessedDataRequest =>
      performProcessedDataPersisting(taskActor, persist)

    case completed: HandleJobCompletedRequest =>
      performJobCompletionHandling(taskActor, completed)
  }


  def performIntegrationValidation(taskActor: ActorRef, input: ValidateIntegrationRequest) : Future[Unit] = {
    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture   = handleCommandException(taskActor, input, Future(simpleCommander.executeCommand(new ValidateIntegrationCommand(input.source))))
    val responseSent   = outputFuture.andThen {
      case Success(CommandOutput(result: ValidateIntegrationResult, logFile)) =>
        val errors  = JavaConversions.asScalaBuffer(result.getValidationErrors).map(e => {
          val trace = Option(e.getException).map(ExecutorException.createIntegrationException)
          ValidationErrorDetails(e.getMessage, trace)
        })
        val response = ValidateIntegrationResponse(
          input   = input,
          logFile = logFile.map { _ => dataHandler.remoteLogLocation(input) },
          isValid = result.isValid,
          schema  = IntegrationSchema(result.getSchema),
          rawDataIntegration  = result.isRawDataIntegration,
          persistingSemantics = result.getPersistingSemantics,
          validationErrors    = errors
        )
        taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
    }

    responseSent.flatMap {
      case CommandOutput(_, logFile) =>
        uploadLogFuture(taskActor, input, logFile)
    }
  }

  def performIngestionTaskCreation(taskActor: ActorRef, input: CreateIngestionTasksRequest) : Future[Unit] = {
    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture = handleCommandException(taskActor, input, Future(simpleCommander.executeCommand(new CreateIngestionTasksCommand(input.job))))

    outputFuture.flatMap {
      case CommandOutput(result: CreateIngestionTasksResult, logFile) =>
        val newTasks  = JavaConversions.asScalaBuffer(TaskAssociations.resetJobReferences(input.job, result.getCreatedTasks))
        val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
        val response  = CreateIngestionTasksResponse(input, remoteLog, newTasks)
        taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
        uploadLogFuture(taskActor, input, logFile)
    }
  }

  def performProcessedDataFetching(taskActor: ActorRef, input: FetchProcessedDataRequest) : Future[Unit] = {

    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture  = handleCommandException(taskActor, input, Future(simpleCommander.executeCommand(new FetchProcessedDataCommand(input.task))))

    outputFuture.flatMap {
      case CommandOutput(result: FetchProcessedDataResult, logFile) =>
        dataHandler.uploadProcessedData(result.getTask, result.getLocalDataFile, result.getRecordCount).map { remoteData =>
          val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
          val response  = FetchProcessedDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
          uploadLogFuture(taskActor, input, logFile)
        }
    }
  }

  def performRawDataFetching(taskActor: ActorRef, input: FetchRawDataRequest) : Future[Unit] = {
    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture = handleCommandException(taskActor, input, Future(simpleCommander.executeCommand(new FetchRawDataCommand(input.task))))

    outputFuture.flatMap {
      case CommandOutput(result: FetchRawDataResult, logFile) =>
        dataHandler.uploadRawData(result.getTask, JavaConversions.asScalaBuffer(result.getRawDataFiles)).map { remoteData =>
          val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
          val response  = FetchRawDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
          uploadLogFuture(taskActor, input, logFile)
        }
    }
  }


  def performRawDataProcessing(taskActor: ActorRef, input: ProcessRawDataRequest) : Future[Unit] = {
    val filesFuture  = handleDownloadFailure(taskActor, input, Future.sequence(input.files.map(dataHandler.download)))
    val outputFuture = filesFuture.map { files =>
      taskActor ! TaskFSMEvent.OperationStarting
      simpleCommander.executeCommand(new ProcessRawDataCommand(input.task, JavaConversions.seqAsJavaList(files)))
    }

    handleCommandException(taskActor, input, outputFuture).flatMap {
      case CommandOutput(result: ProcessRawDataResult, logFile) =>
        dataHandler.uploadProcessedData(result.getTask, result.getLocalDataFile, result.getRecordCount).map { remoteData =>
          val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
          val response  = ProcessRawDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
          uploadLogFuture(taskActor, input, logFile)
        }
    }
  }

  def performProcessedDataPersisting(taskActor: ActorRef, input: PersistProcessedDataRequest) : Future[Unit] = {
    val fileFuture   = handleDownloadFailure(taskActor, input, dataHandler.download(input.data))
    val outputFuture = fileFuture.map { data =>
      taskActor ! TaskFSMEvent.OperationStarting
      simpleCommander.executeCommand(new PersistProcessedDataCommand(input.task, data))
    }
    handleCommandException(taskActor, input, outputFuture).flatMap {
      case CommandOutput(result: PersistProcessedDataResult, logFile) =>
        val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
        val response  = PersistProcessedDataResponse(input, remoteLog)
        taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
        uploadLogFuture(taskActor, input, logFile)
    }
  }

  def performJobCompletionHandling(taskActor: ActorRef, input: HandleJobCompletedRequest) : Future[Unit] = {
    taskActor ! TaskFSMEvent.OperationStarting

    val command      = new HandleJobCompletedCommand(input.completedAt, input.job, JavaConversions.seqAsJavaList(input.tasks))
    val outputFuture = handleCommandException(taskActor, input, Future(simpleCommander.executeCommand(command)))

    outputFuture.flatMap {
      case CommandOutput(result: HandleJobCompletedResult, logFile) =>
        val remoteLog = logFile.map { _ => dataHandler.remoteLogLocation(input) }
        val response  = HandleJobCompletedResponse(input, remoteLog)
        taskActor ! TaskFSMEvent.OperationResponseAvailable(response)
        uploadLogFuture(taskActor, input, logFile)
    }
  }

  def handleDownloadFailure[T](taskActor: ActorRef, input: WorkerInput, download: Future[T]): Future[T] = {
    download.andThen {
      case Success(_) =>
        log.debug("Finished downloading dependencies for task {}", input.summaryString)
      case Failure(e) =>
        log.error(e, "Failure while downloading dependencies for task {}", input.summaryString)
        taskActor ! TaskFSMEvent.TaskDependencyFailure(e)
        self ! Failure(e)
    }
  }

  def handleCommandException(taskActor: ActorRef, input: WorkerInput, future: Future[CommandOutput]): Future[CommandOutput] = {
    future.andThen {
      case Failure(e: CommandExecutionException) =>
        log.error("Failed executing task {} - {}", input.summaryString, e.error.summary)
        val remoteLog = e.executorLog.map(_ => dataHandler.remoteLogLocation(input))
        taskActor ! TaskFSMEvent.OperationResponseAvailable(FailureResponse(input, remoteLog, Some(e.error)))
        uploadLogFuture(taskActor, input, e.executorLog).andThen {
          case _ => self ! Failure(e)
        }
      //  Still bail on unexpected failure case.
      case Failure(e) =>
        log.error(e, "Encountered unexpected failure for {} instead of command exception", input.summaryString)
        self ! Failure(e)
    }
  }

  def uploadLogFuture(taskActor: ActorRef, task: WorkerInput, taskLog: Option[File]): Future[Unit] = {
    if (!config.uploadTaskLog || taskLog.isEmpty){
      Future[Unit]({
        log.debug("Log uploads are disabled. Notifying task actor of completion")
        taskActor ! TaskFSMEvent.OperationLogUploaded
      })
    } else {
      import akka.pattern.after
      val logFile  = taskLog.get
      val timeout  = after(config.uploadLogTimeout, context.system.scheduler)(Future.failed[RemoteLogFile](new TimeoutException("Maximum duration for log upload exceeded")))
      val complete = dataHandler.uploadLogFile(task, logFile)
      Future.firstCompletedOf(Seq(timeout, complete)).andThen {
        case Success(uploaded) =>
          log.debug("Successfully uploaded log for task {} to s3://{}/{}", task.executionId, uploaded.location.bucket, uploaded.location.key)
        case Failure(e) =>
          log.error(e, "Failed to upload log file for task {}", task.executionId)
      }.andThen {
        case _ =>
          taskActor ! TaskFSMEvent.OperationLogUploaded
          simpleCommander.executor.files.cleanupLogs()
      }.map { _ => () }
    }
  }

  def assertIntegrationCodeMatchesJars() : Unit = {
    val codeKeys = jarFiles.map(_.key)
    if (integration.jarFiles != codeKeys){
      throw new IllegalStateException(s"Commander received jar file mismatch for $integration with jars: ${ codeKeys.mkString(", ") }")
    }
  }
}
