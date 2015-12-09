package com.harrys.hyppo.worker.actor

import akka.actor._
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.WorkerFSM._
import com.harrys.hyppo.worker.actor.queue.WorkQueueExecution
import com.harrys.hyppo.worker.actor.task.TaskFSM
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.{GeneralWorkerInput, IntegrationWorkerInput}
import com.harrys.hyppo.worker.data.{LoadedJarFile, JarLoadingActor}
import com.rabbitmq.client.{ShutdownListener, ShutdownSignalException}
import com.thenewmotion.akka.rabbitmq._
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure

/**
 * Created by jpetty on 10/30/15.
 */
final class WorkerFSM(config: WorkerConfig, delegator: ActorRef, connection: ActorRef) extends LoggingFSM[WorkerState, CommanderState] {

  val jarLoadingActor = context.watch(context.actorOf(Props(classOf[JarLoadingActor], config)))
  val channelActor    = {
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    context.watch(connection.createChannel(ChannelActor.props(initializeWorkerChannel)))
  }

  override def logDepth: Int = 10

  startWith(Idle, Uninitialized)

  /**
    * [[WorkerFSM.Idle]] State Definition
    */
  when (Idle) {
    case Event(execution: WorkQueueExecution, _) =>
      goto(LoadingCode) using WaitingForJars(execution)
    case Event(RequestWorkEvent, _) =>
      delegator ! RequestForAnyWork(channelActor)
      stay()
    case Event(cause: ShutdownSignalException, _) =>
      log.warning(s"Received channel shutdown message: ${ cause.getMessage }")
      stay()
  }

  //  When loading code, the WorkerFSM must initiate the request to download the necessary jars
  onTransition {
    case _ -> LoadingCode =>
      val execution = nextStateData.asInstanceOf[WaitingForJars].execution
      jarLoadingActor ! JarLoadingActor.LoadJars(execution.input.code.jarFiles)
  }


  /***
    * [[WorkerFSM.LoadingCode]] Status Definition Block
    */
  when (LoadingCode, stateTimeout = config.jarDownloadTimeout) {
    case Event(JarLoadingActor.JarsResult(jars), WaitingForJars(execution)) =>
      if (jars.map(_.key) == execution.input.code.jarFiles){
        log.debug(s"Code successfully loaded. Launching commander")
        goto(Running) using createCommanderActor(execution, jars)
      } else {
        log.warning("Received unexpected code from jar loading actor. Deleting files and still waiting")
        jars.foreach(j => FileUtils.deleteQuietly(j.file))
        stay()
      }

    case Event(channelError: ShutdownSignalException, WaitingForJars(execution)) =>
      log.error(channelError, "Channel closed while loading code. Resources are automatically, released. Going to idle.")
      goto(Idle) using Uninitialized

    case Event(Failure(cause), WaitingForJars(execution)) =>
      log.debug("Failed to load code for commander. Returning to idle", cause)
      execution.withChannel(c => {
        c.basicReject(execution.headers.deliveryTag, true)
        execution.leases.releaseAll()
      })
      goto(Idle) using Uninitialized

    case Event(Lifecycle.ImpendingShutdown, WaitingForJars(execution)) =>
      log.debug("Failed to load code due to impending shutdown")
      execution.withChannel(c => {
        c.basicReject(execution.headers.deliveryTag, true)
        execution.leases.releaseAll()
      })
      stop(FSM.Shutdown)

    case Event(StateTimeout, WaitingForJars(execution)) =>
      log.debug("Code loading timed out. Work will return to queue automatically")
      execution.withChannel(c => {
        c.basicReject(execution.headers.deliveryTag, true)
        execution.leases.releaseAll()
      })
      goto(Idle) using Uninitialized
  }


  /**
    * [[WorkerFSM.Running]] State Definition
    */
  when (Running, stateTimeout = config.workTimeout) {
    case Event(RequestWorkEvent, _) =>
      log.warning("Unexpected RequestWorkEvent received while processing. Likely a race-condition inside mailbox.")
      stay()

    case Event(Terminated(stopped), active: ActiveCommander) if stopped.equals(active.taskActor) =>
      log.debug("Task actor exited successfully")
      active.workPreference match {
        case Some(prefer) =>
          goto(Available) using active.copy(taskActor = null)
        case None =>
          context.stop(active.commander)
          goto(Idle) using Uninitialized
      }

    case Event(channelError: ShutdownSignalException, active: ActiveCommander) =>
      log.error(channelError, "Channel closed while work was in progress. Killing commander.")
      context.unwatch(active.taskActor)
      context.stop(context.unwatch(active.commander))
      goto(Idle) using Uninitialized

    case Event(StateTimeout, active: ActiveCommander) =>
      log.error("Current work timed out in running state. Transitioning back to idle")
      context.unwatch(active.taskActor)
      context.stop(active.commander)
      goto(Idle) using Uninitialized
  }

  /**
    * [[WorkerFSM.Available]] State Definition
    */
  when (Available) {
    case Event(RequestWorkEvent, active: ActiveCommander) =>
      active.workPreference match {
        case Some(prefer) =>
          delegator ! RequestForPreferredWork(channelActor, prefer)
          stay()

        case None =>
          log.debug("Losing current work affinity and transitioning to idle state")
          context.stop(active.commander)
          goto(Idle) using Uninitialized
      }

    case Event(channelError: ShutdownSignalException, active: ActiveCommander) =>
      log.error(channelError, "Channel closed while in available state. Transitioning to idle until channel stabilizes")
      context.stop(context.unwatch(active.commander))
      goto(Idle) using Uninitialized

    case Event(item @ WorkQueueExecution(_, _, input: IntegrationWorkerInput, _), active @ ActiveCommander(commander, _, Some(affinity))) =>
      if (active.isSameCode(input.integration)){
        log.debug(s"Reusing previous commander for pre-loaded integration: ${affinity.integration.sourceName}")
        val taskActor = createTaskActor(item, commander)
        goto(Running) using ActiveCommander(commander, taskActor, Some(affinity))
      } else {
        log.debug(s"Restarting commander for fresh integration: ${ input.integration.sourceName }")
        context.stop(commander)
        goto(LoadingCode) using WaitingForJars(item)
      }

    case Event(item @ WorkQueueExecution(_, _, input: GeneralWorkerInput, _), ActiveCommander(commander, _, _)) =>
      log.debug(s"Restarting commander for general work request: ${input.integration.sourceName}")
      context.stop(commander)
      goto(LoadingCode) using WaitingForJars(item)

  }

  whenUnhandled {
    case Event(JarLoadingActor.JarsResult(jars), _) =>
      log.warning(s"Received unexpected jar loading result. Deleting jars and staying in current state: $stateName")
      jars.foreach(j => FileUtils.deleteQuietly(j.file))
      stay()

    case Event(Lifecycle.ImpendingShutdown, active: ActiveCommander) =>
      log.error("Received impending shutdown event")
      stop()

    case Event(Lifecycle.ImpendingShutdown, state) =>
      log.error(s"Received impending shutdown while in state: ${state.toString}")
      stop()

    case Event(msg, active: ActiveCommander) =>
      stop(FSM.Failure(s"Shutting down. Unexpected message received: ${msg.toString}"))

    case Event(msg, _) =>
      stop(FSM.Failure(s"Shutting down. Unexpected message received: ${msg.toString}"))
  }

  onTermination {
    case StopEvent(_, LoadingCode, WaitingForJars(execution)) =>
      stopPollingForWork()
      context.stop(context.unwatch(jarLoadingActor))
      context.stop(context.unwatch(channelActor))
      log.info(s"WorkerFSM stopped successfully from state: ${ stateName }")

    case StopEvent(_, _, active: ActiveCommander) =>
      stopPollingForWork()
      context.stop(context.unwatch(jarLoadingActor))
      Option(active.taskActor).foreach(a => context.unwatch(a))
      Await.result(gracefulStop(active.commander, config.workerShutdownTimeout), config.workerShutdownTimeout)
      context.stop(context.unwatch(channelActor))
      log.info(s"WorkerFSM stopped successfully from state: ${ stateName }")

    case StopEvent(_, _, _) =>
      stopPollingForWork()
      context.stop(context.unwatch(jarLoadingActor))
      context.stop(context.unwatch(channelActor))
      log.info(s"WorkerFSM stopped successfully from state: ${ stateName }")
  }

  onTransition {
    case _ -> Idle        => startPollingForWork()
    case _ -> LoadingCode => stopPollingForWork()
    case _ -> Available   => startPollingForWork()
    case _ -> Running     => stopPollingForWork()
  }

  initialize()
  startPollingForWork()

  def startPollingForWork() : Unit = if (!isTimerActive(PollingTimerName)){
    self ! RequestWorkEvent
    setTimer(PollingTimerName, RequestWorkEvent, config.taskPollingInterval, repeat = true)
  }

  def stopPollingForWork() : Unit = if (isTimerActive(PollingTimerName)){
    cancelTimer(PollingTimerName)
  }

  private var commanderCounter = 0

  def createTaskActor(item: WorkQueueExecution, commander: ActorRef) : ActorRef = {
    context.watch(context.actorOf(Props(classOf[TaskFSM], config, item, commander), name = "task-" + item.input.executionId.toString))
  }

  def createCommanderActor(execution: WorkQueueExecution, jarFiles: Seq[LoadedJarFile]) : ActiveCommander = {
    commanderCounter += 1
    val commander = context.actorOf(Props(classOf[CommanderActor], config, execution.input.code, jarFiles), name = "commander-" + commanderCounter)
    val taskActor = createTaskActor(execution, commander)
    execution.input match {
      case work: IntegrationWorkerInput =>
        val affinity = WorkerAffinity(work.integration, Deadline.now + config.workAffinityTimeout)
        ActiveCommander(commander, taskActor, Some(affinity))

      case work: GeneralWorkerInput =>
        ActiveCommander(commander, taskActor, None)
    }
  }

  def initializeWorkerChannel(channel: Channel, actor: ActorRef) : Unit = {
    channel.addShutdownListener(new ShutdownListener {
      override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
        self ! cause
      }
    })
  }
}

object WorkerFSM {
  val PollingTimerName = "polling"

  sealed trait WorkerState
  case object Idle extends WorkerState
  case object Running extends WorkerState
  case object Available extends WorkerState
  case object LoadingCode extends WorkerState


  final case class WorkerAffinity(integration: ExecutableIntegration, expiration: Deadline) {
    def isExpired() : Boolean = expiration.isOverdue()
  }

  sealed trait CommanderState

  case object Uninitialized extends CommanderState

  final case class WaitingForJars(execution: WorkQueueExecution) extends CommanderState

  final case class ActiveCommander
  (
    commander:  ActorRef,
    taskActor:  ActorRef,
    affinity:   Option[WorkerAffinity]
  ) extends CommanderState {

    def workPreference: Option[ExecutableIntegration] = affinity match {
      case Some(value) if !value.isExpired() => Some(value.integration)
      case other => None
    }

    def isSameCode(integration: ExecutableIntegration) : Boolean = affinity match {
      case Some(details) => details.integration.isSameCodeBase(integration)
      case None          => false
    }
  }

  case object RequestWorkEvent
}
