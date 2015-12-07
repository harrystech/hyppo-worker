package com.harrys.hyppo.worker.actor

import java.io.File
import java.net.{InetAddress, ServerSocket}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.executor.cli.ExecutorMain
import com.harrys.hyppo.executor.proto.com._
import com.harrys.hyppo.executor.proto.res._
import com.harrys.hyppo.source.api.model.{DataIngestionJob, TaskAssociations}
import com.harrys.hyppo.worker.actor.task.TaskFSMEvent
import com.harrys.hyppo.worker.api.code.{IntegrationCode, IntegrationSchema}
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.cache.LoadedJarFile
import com.harrys.hyppo.worker.data.{DataHandler, TempFilePool}
import com.harrys.hyppo.worker.proc.{CommandOutput, ExecutorException, SimpleCommander}

import scala.collection.JavaConversions
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Success, Try}

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
    val executor = setup.launchWithArgs(socket.getLocalPort, integration.integrationClass, config.taskLogStrategy)
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
    Try(simpleCommander.sendExitCommandAndWaitThenKill(Duration(1, SECONDS)))
    Try(simpleCommander.executor.deleteFiles())
  }

  private case object WorkCompletedMessage

  private var isRunningWork = false

  override def receive: Receive = {
    case WorkCompletedMessage =>
      if (!isRunningWork){
        throw new IllegalStateException(s"CommanderActor should never receive ${ WorkCompletedMessage.productPrefix } when not running work")
      }
      isRunningWork = false
      log.debug(s"Commander ${ self.path.name } is now available")

    case input: WorkerInput =>
      if (isRunningWork){
        throw new IllegalStateException("Executor should never receive work while still running previous jobs!")
      }
      log.debug(s"Commander ${ self.path.name } starting work on ${ input.summaryString }")
      isRunningWork  = true
      executeWorkRequest(sender(), input).onComplete {
        case _ => self ! WorkCompletedMessage
      }
  }


  def executeWorkRequest(taskActor: ActorRef, input: WorkerInput) : Future[Unit] = input match {
    case validate: ValidateIntegrationRequest =>
      performIntegrationValidation(sender(), validate)

    case create: CreateIngestionTasksRequest =>
      performIngestionTaskCreation(sender(), create)

    case fetch: FetchProcessedDataRequest =>
      performProcessedDataFetching(sender(), fetch)

    case fetch: FetchRawDataRequest =>
      performRawDataFetching(sender(), fetch)

    case process: ProcessRawDataRequest =>
      performRawDataProcessing(sender(), process)

    case persist: PersistProcessedDataRequest =>
      performProcessedDataPersisting(sender(), persist)

    case completed: HandleJobCompletedRequest =>
      performJobCompletionHandling(sender(), completed)
  }


  def performIntegrationValidation(taskActor: ActorRef, input: ValidateIntegrationRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)

    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture = Future(simpleCommander.executeCommand(new ValidateIntegrationCommand(input.source)))
    val responseSent = outputFuture.andThen {
      //  Construct the response message and send it to the task actor
      case Success(CommandOutput(result: ValidateIntegrationResult, logFile)) =>
        val errors = JavaConversions.asScalaBuffer(result.getValidationErrors).map(e => {
          val trace = Option(e.getException).map(ExecutorException.createIntegrationException)
          ValidationErrorDetails(e.getMessage, trace)
        })
        val response = ValidateIntegrationResponse(
          input   = input,
          logFile = remoteLog,
          isValid = result.isValid,
          schema  = IntegrationSchema(result.getSchema),
          rawDataIntegration  = result.isRawDataIntegration,
          persistingSemantics = result.getPersistingSemantics,
          validationErrors = errors
        )
        taskActor ! TaskFSMEvent.OperationResultAvailable(response)
    }

    responseSent.flatMap {
      case CommandOutput(_, logFile) =>
        createLogUploadFuture(taskActor, remoteLog, logFile)
    }
  }

  def performIngestionTaskCreation(taskActor: ActorRef, input: CreateIngestionTasksRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)

    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture = Future(simpleCommander.executeCommand(new CreateIngestionTasksCommand(input.job)))

    outputFuture.flatMap {
      case CommandOutput(result: CreateIngestionTasksResult, logFile) =>
        val newTasks  = JavaConversions.asScalaBuffer(TaskAssociations.resetJobReferences(input.job, result.getCreatedTasks))
        val response  = CreateIngestionTasksResponse(input, remoteLog, newTasks)
        taskActor ! TaskFSMEvent.OperationResultAvailable(response)
        createLogUploadFuture(taskActor, remoteLog, logFile)
    }
  }

  def performProcessedDataFetching(taskActor: ActorRef, input: FetchProcessedDataRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)

    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture  = Future(simpleCommander.executeCommand(new FetchProcessedDataCommand(input.task)))

    outputFuture.flatMap {
      case CommandOutput(result: FetchProcessedDataResult, logFile) =>
        dataHandler.uploadProcessedData(result.getTask, result.getLocalDataFile, result.getRecordCount).map { remoteData =>
          val response = FetchProcessedDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResultAvailable(response)
          createLogUploadFuture(taskActor, remoteLog, logFile)
        }
    }
  }

  def performRawDataFetching(taskActor: ActorRef, input: FetchRawDataRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)

    taskActor ! TaskFSMEvent.OperationStarting

    val outputFuture = Future(simpleCommander.executeCommand(new FetchRawDataCommand(input.task)))

    outputFuture.flatMap {
      case CommandOutput(result: FetchRawDataResult, logFile) =>
        dataHandler.uploadRawData(result.getTask, JavaConversions.asScalaBuffer(result.getRawDataFiles)).map { remoteData =>
          val response = FetchRawDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResultAvailable(response)
          createLogUploadFuture(taskActor, remoteLog, logFile)
        }
    }
  }


  def performRawDataProcessing(taskActor: ActorRef, input: ProcessRawDataRequest) : Future[Unit] = {
    val remoteLog   = dataHandler.remoteLogLocation(input)
    val filesFuture = Future.sequence(input.files.map(dataHandler.download)).andThen {
      case Success(_) =>
        taskActor ! TaskFSMEvent.OperationStarting
    }
    val outputFuture = filesFuture.map { files =>
      simpleCommander.executeCommand(new ProcessRawDataCommand(input.task, JavaConversions.seqAsJavaList(files)))
    }

    outputFuture.flatMap {
      case CommandOutput(result: ProcessRawDataResult, logFile) =>
        dataHandler.uploadProcessedData(result.getTask, result.getLocalDataFile, result.getRecordCount).map { remoteData =>
          val response = ProcessRawDataResponse(input, remoteLog, remoteData)
          taskActor ! TaskFSMEvent.OperationResultAvailable(response)
          createLogUploadFuture(taskActor, remoteLog, logFile)
        }
    }
  }

  def performProcessedDataPersisting(taskActor: ActorRef, input: PersistProcessedDataRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)
    val fileFuture = dataHandler.download(input.data).andThen {
      case Success(_) =>
        taskActor ! TaskFSMEvent.OperationStarting
    }
    val outputFuture = fileFuture.map { data =>
      simpleCommander.executeCommand(new PersistProcessedDataCommand(input.task, data))
    }
    outputFuture.flatMap {
      case CommandOutput(result: PersistProcessedDataResult, logFile) =>
        val response = PersistProcessedDataResponse(input, remoteLog)
        taskActor ! TaskFSMEvent.OperationResultAvailable(response)
        createLogUploadFuture(taskActor, remoteLog, logFile)
    }
  }

  def performJobCompletionHandling(taskActor: ActorRef, input: HandleJobCompletedRequest) : Future[Unit] = {
    val remoteLog  = dataHandler.remoteLogLocation(input)

    taskActor ! TaskFSMEvent.OperationStarting

    val command      = new HandleJobCompletedCommand(input.completedAt, input.job, JavaConversions.seqAsJavaList(input.tasks))
    val outputFuture = Future(simpleCommander.executeCommand(command))

    outputFuture.flatMap {
      case CommandOutput(result: HandleJobCompletedResult, logFile) =>
        val response = HandleJobCompletedResponse(input, remoteLog)
        taskActor ! TaskFSMEvent.OperationResultAvailable(response)
        createLogUploadFuture(taskActor, remoteLog, logFile)
    }
  }

  def createLogUploadFuture(taskActor: ActorRef, logLocation: RemoteLogFile, taskLog: File) : Future[Unit] = {
    val uploadFuture: Future[RemoteLogFile] =
      if (config.uploadTaskLog){
        //  This always succeeds, even when it doesn't because it should never prevent forward progress
        dataHandler.uploadLogFile(logLocation, taskLog).recover {
          case e: Exception =>
            log.error(e, s"Failed to upload log file: ${logLocation.toString}")
            logLocation
        }
      } else {
        Future.successful[RemoteLogFile](logLocation)
      }

    val timeout = akka.pattern.after(config.uploadLogTimeout, context.system.scheduler)(Future.successful(logLocation))
    //  Always signal the actor about the upload completion
    Future.firstCompletedOf(Seq(uploadFuture, timeout)).andThen {
      case _ =>
        taskActor ! TaskFSMEvent.OperationLogUploaded
        simpleCommander.executor.files.cleanupLogs()
    }.map { _ => () } //  This converts the future into that Future[Unit]. Don't worry about it.
  }

  def assertIntegrationCodeMatchesJars() : Unit = {
    val codeKeys = jarFiles.map(_.key)
    if (integration.jarFiles != codeKeys){
      throw new IllegalStateException(s"Commander ${ integration.toString } is not usable with ${ codeKeys.toString }")
    }
  }
}
