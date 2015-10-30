package com.harrys.hyppo.worker.actor

import akka.actor._
import akka.pattern.{ask, gracefulStop}
import akka.util.Timeout
import com.github.sstone.amqp.Amqp.{Error, Ok}
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.WorkerFSM._
import com.harrys.hyppo.worker.actor.amqp.WorkQueueItem
import com.harrys.hyppo.worker.actor.task.TaskFSM
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.{GeneralWorkerInput, IntegrationWorkerInput}
import com.harrys.hyppo.worker.cache.{JarLoadingActor, LoadedJarFile}
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure

/**
 * Created by jpetty on 10/30/15.
 */
final class WorkerFSM(config: WorkerConfig, delegator: ActorRef) extends LoggingFSM[WorkerState, CommanderState] {

  val jarLoadingActor = context.watch(context.actorOf(Props(classOf[JarLoadingActor], config)))

  override def logDepth: Int = 10

  startWith(Idle, Uninitialized)

  when (Idle) {

    case Event(work: WorkQueueItem, _) =>
      jarLoadingActor ! JarLoadingActor.LoadJars(work.input.code.jarFiles)
      goto(LoadingCode) using WaitingForJars(work)

    case Event(RequestWorkEvent, _) =>
      delegator ! RequestForAnyWork
      stay()

  }

  when (LoadingCode, stateTimeout = config.jarDownloadTimeout) {
    case Event(JarLoadingActor.JarsResult(jars), WaitingForJars(work)) =>
      if (jars.map(_.key) == work.input.code.jarFiles){
        log.debug(s"Code successfully loaded. Launching commander")
        goto(Running) using createCommanderActor(work, jars)
      } else {
        log.warning("Received unexpected code from jar loading actor. Deleting files and still waiting")
        jars.foreach(j => FileUtils.deleteQuietly(j.file))
        stay()
      }

    case Event(Failure(cause), WaitingForJars(work)) =>
      log.debug("Failed to load code for commander", cause)
      sendNackToWorkItem(work)
      goto(Idle) using Uninitialized

    case Event(StateTimeout, WaitingForJars(work)) =>
      log.debug("Code loading timed out")
      sendNackToWorkItem(work)
      goto(Idle) using Uninitialized
  }

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

    case Event(StateTimeout, active: ActiveCommander) =>
      log.error("Current work timed out in running state. Transitioning back to idle")
      context.unwatch(active.taskActor)
      context.stop(active.commander)
      goto(Idle) using Uninitialized
  }

  when (Available) {

    case Event(RequestWorkEvent, active: ActiveCommander) =>
      active.workPreference match {
        case Some(prefer) =>
          delegator ! RequestForPreferredWork(prefer)
          stay()

        case None =>
          log.debug("Losing current work affinity and transitioning to idle state")
          context.stop(active.commander)
          goto(Idle) using Uninitialized
      }

    case Event(item @ WorkQueueItem(_, input: IntegrationWorkerInput), active @ ActiveCommander(commander, _, Some(affinity))) =>
      if (active.isSameCode(input.integration)){
        log.debug(s"Reusing previous commander for pre-loaded integration: ${affinity.integration.sourceName}")
        val taskActor = createTaskActor(item, commander)
        goto(Running) using ActiveCommander(commander, taskActor, Some(affinity))
      } else {
        log.debug(s"Restarting commander for fresh integration: ${ input.integration.sourceName }")
        context.stop(commander)
        goto(LoadingCode) using WaitingForJars(item)
      }

    case Event(item @ WorkQueueItem(_, input: GeneralWorkerInput), ActiveCommander(commander, _, _)) =>
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
    case StopEvent(_, _, active: ActiveCommander) =>
      stopPollingForWork()
      context.stop(context.unwatch(jarLoadingActor))
      Option(active.taskActor).map( context.unwatch(_) )
      Await.result(gracefulStop(active.commander, Duration(5, SECONDS)), Duration(5, SECONDS))
      log.info("WorkerFSM stopped successfully")

    case StopEvent(_, _, _) =>
      stopPollingForWork()
      context.stop(context.unwatch(jarLoadingActor))
      log.info("WorkerFSM stopped successfully")
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

  def createTaskActor(item: WorkQueueItem, commander: ActorRef) : ActorRef = {
    context.watch(context.actorOf(Props(classOf[TaskFSM], config, item, commander)))
  }

  def createCommanderActor(item: WorkQueueItem, jarFiles: Seq[LoadedJarFile]) : ActiveCommander = {
    commanderCounter += 1
    val commander = context.actorOf(Props(classOf[CommanderActor], config, item.input.code, jarFiles), name = "commander-" + commanderCounter)
    val taskActor = createTaskActor(item, commander)
    item.input match {
      case work: IntegrationWorkerInput =>
        val affinity = WorkerAffinity(work.integration, Deadline.now + config.workAffinityTimeout)
        ActiveCommander(commander, taskActor, Some(affinity))

      case work: GeneralWorkerInput =>
        ActiveCommander(commander, taskActor, None)
    }
  }

  def sendNackToWorkItem(item: WorkQueueItem) : Unit = {
    import context.dispatcher
    implicit val timeout = Timeout(config.rabbitMQTimeout)
    (item.rabbitItem.channel ? item.rabbitItem.createNack(requeue = true)).collect {
      case Ok(_, _) =>
        log.debug("Successfully sent NACK to Rabbit")
      case Error(_, cause) =>
        log.error(cause, "Failed to NACK work queue item")
        self ! Failure(cause)
    }
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

  final case class WaitingForJars(item: WorkQueueItem) extends CommanderState

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
