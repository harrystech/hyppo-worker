package com.harrys.hyppo.worker.actor

import java.io.File
import java.net.{InetAddress, ServerSocket}

import akka.actor._
import akka.util.Timeout
import com.github.sstone.amqp.Amqp.Ack
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.executor.cli.ExecutorMain
import com.harrys.hyppo.executor.proto.com._
import com.harrys.hyppo.executor.proto.res._
import com.harrys.hyppo.executor.proto.{OperationResult, StartOperationCommand}
import com.harrys.hyppo.source.api.model.{DataIngestionJob, DataIngestionTask}
import com.harrys.hyppo.worker.actor.amqp.WorkQueueItem
import com.harrys.hyppo.worker.api.code.{IntegrationCode, IntegrationSchema}
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.cache.LoadedJarFile
import com.harrys.hyppo.worker.data.{DataHandler, TempFilePool}
import com.harrys.hyppo.worker.proc.{ExecutorException, SimpleCommander}

import scala.collection.JavaConversions
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success}

/**
 * Created by jpetty on 8/3/15.
 */
final class CommanderActor
(
  config:       WorkerConfig,
  integration:  IntegrationCode,
  jarFiles:     Seq[LoadedJarFile]
) extends Actor with ActorLogging
{

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
    val executor = setup.launchWithArgs(socket.getLocalPort, integration.integrationClass)
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
    simpleCommander.forceShutdown()
    simpleCommander.executor.deleteFiles()
  }

  //  Internally used to trigger a restart while firing off a response to the task actor
  private final case class CommandFailure(taskActor: ActorRef, response: FailureResponse)

  //  Internally used to consolidate the failure / success handling of the task actor to a single point
  private final case class CommandComplete(taskActor: ActorRef, item: WorkQueueItem)

  override def receive : Receive = {
    case CommandComplete(taskActor, item) =>
      log.debug(s"Successfully completed work item ${ item.input.toString }")
      taskActor ! PoisonPill

    case CommandFailure(taskActor, response) =>
      val detail = response.failure.map(_.summary).getOrElse("Unknown error")
      log.error(s"Restarting executor due to internal failure: ${ detail }")
      taskActor ! response
      taskActor ! PoisonPill
      throw new Exception(detail)

    case item: WorkQueueItem =>
      val taskActor = sender()

      log.info(s"Starting work on Rabbit item ${ item.rabbitItem.printableDetails }")
      Future({
        val doneFuture = item match {
          case WorkQueueItem(_, _: PersistProcessedDataRequest) =>
            executePersistingWork(item, taskActor)
          case _ =>
            executeWorkRequest(item, taskActor)
        }
        //  Converts all future chains into a synchronous call to allow for actor restarting to clean up the
        //  environment without mailbox sync issues.
        try {
          Await.result(doneFuture, config.workTimeout)
          self ! CommandComplete(taskActor, item)
        } catch {
          case e: ExecutorException =>
            log.error(e, "Failure inside of executor")
            self ! CommandFailure(taskActor, FailureResponse(item.input, Some(e.toIntegrationException)))
          case e: Exception =>
            log.error(e, "Failure executing executor command")
            self ! CommandFailure(taskActor, FailureResponse(item.input, Some(IntegrationException.fromThrowable(e))))
        } finally {
          tempFiles.cleanAll()
        }
      })
  }


  def executeWorkRequest(item: WorkQueueItem, taskActor: ActorRef) : Future[Any] = {
    implicit val timeout = Timeout(config.workTimeout)

    val responseFuture = createCommanderOperation(item).map { command =>
      simpleCommander.executeCommand(command)
    }.flatMap { result =>
      createWorkerResponse(item.input, result)
    }

    responseFuture.andThen {
      case Success(result) =>
        taskActor ! result
        taskActor ! Ack(item.rabbitItem.deliveryTag)

      case Failure(error) =>
        taskActor ! Ack(item.rabbitItem.deliveryTag)
    }
  }


  def executePersistingWork(item: WorkQueueItem, taskActor: ActorRef) : Future[Any] = {
    val deserialized  = item.input.asInstanceOf[PersistProcessedDataRequest]
    val remappedTasks = deserialized.data.map(d => d.copy(task = new DataIngestionTask(deserialized.job, d.task.getTaskNumber, d.task.getTaskArguments)))
    //  This is necessary to avoid passing tasks to the executor with a null job field. The values end up here
    // with a null to avoid serialization overhead of repeated values
    val persist = deserialized.copy(data = remappedTasks)

    //  Internal method that represents the persisting behavior of a single task processed data file
    def singlePersistenceFuture(data: ProcessedTaskData, ack: Boolean = false) : Future[Unit] = {
      dataHandler.download(data.file).map { file =>
        try {
          //  Ack is initiated right before the first task is passed to the executor
          if (ack){
            taskActor ! item.rabbitItem.createAck()
          }
          simpleCommander.executeCommand(new PersistProcessedDataCommand(data.task, file))
          taskActor ! PersistProcessedDataResponse(persist, data)
        } finally {
          tempFiles.cleanAll()
        }
      }
    }

    //  This distinction allows us to defer the initial ACK until the first data file has been downloaded
    if (persist.data.nonEmpty){
      val first = singlePersistenceFuture(persist.data.head, ack = true)
      persist.data.tail.foldLeft(first) { (future, data) =>
        future.flatMap(_ => singlePersistenceFuture(data, ack = false))
      }
    } else {
      taskActor ! item.rabbitItem.createAck()
      Future.successful(Nil)
    }
  }

  def createCommanderOperation(item: WorkQueueItem) : Future[StartOperationCommand] = item.input match {
    case validate:  ValidateIntegrationRequest  => Future.successful(new ValidateIntegrationCommand(validate.integration.source))
    case create:    CreateIngestionTasksRequest => Future.successful(new CreateIngestionTasksCommand(create.job))
    case fetch:     FetchProcessedDataRequest   => Future.successful(new FetchProcessedDataCommand(fetch.task))
    case fetch:     FetchRawDataRequest         => Future.successful(new FetchRawDataCommand(fetch.task))
    case process:   ProcessRawDataRequest       =>
      val filesFuture = Future.sequence(process.files.map(dataHandler.download))
      filesFuture.map { files =>
        new ProcessRawDataCommand(process.task, JavaConversions.seqAsJavaList(files))
      }
    case persist:   PersistProcessedDataRequest => Future.failed(new IllegalStateException("Persisting operations should never fall through to this block!"))
  }


  def createWorkerResponse(input: WorkerInput, result: OperationResult) : Future[WorkerResponse] = result match {
    case validate:  ValidateIntegrationResult   =>
      val request  = input.asInstanceOf[ValidateIntegrationRequest]
      val errors   = JavaConversions.asScalaBuffer(validate.getValidationErrors).map(e => {
        val trace  = Option(e.getException).map(ExecutorException.createIntegrationException)
        ValidationErrorDetails(e.getMessage, trace)
      })
      val response = ValidateIntegrationResponse(
        input   = request,
        isValid = validate.isValid,
        schema  = IntegrationSchema(validate.getSchema),
        rawDataIntegration  = validate.isRawDataIntegration,
        persistingSemantics = validate.getPersistingSemantics,
        validationErrors = errors
      )
      Future.successful(response)
    case create:    CreateIngestionTasksResult  => {
      val request  = input.asInstanceOf[CreateIngestionTasksRequest]
      val response = CreateIngestionTasksResponse(request, JavaConversions.asScalaBuffer(create.getCreatedTasks))
      Future.successful(response)
    }
    case fetch:     FetchProcessedDataResult    => {
      val request  = input.asInstanceOf[FetchProcessedDataRequest]
      dataHandler.uploadProcessedData(request.task, fetch.getLocalDataFile, fetch.getRecordCount).map { remoteFile =>
        FetchProcessedDataResponse(request, remoteFile)
      }
    }
    case fetch:     FetchRawDataResult          => {
      val request = input.asInstanceOf[FetchRawDataRequest]
      dataHandler.uploadRawData(request.task, JavaConversions.asScalaBuffer(fetch.getRawDataFiles)).map { remoteFiles =>
        FetchRawDataResponse(request, remoteFiles)
      }
    }
    case process:   ProcessRawDataResult        => {
      val request = input.asInstanceOf[ProcessRawDataRequest]
      dataHandler.uploadProcessedData(request.task, process.getLocalDataFile, process.getRecordCount).map { remoteFile =>
        ProcessRawDataResponse(request, remoteFile)
      }
    }
    case persist: PersistProcessedDataResult  => Future.failed(new IllegalStateException("Persisting operations should never fall through to this block!"))
  }


  def assertIntegrationCodeMatchesJars() : Unit = {
    val codeKeys = jarFiles.map(_.key)
    if (integration.jarFiles != codeKeys){
      throw new IllegalStateException(s"Commander ${ integration.toString } is not usable with ${ codeKeys.toString }")
    }
  }
}
