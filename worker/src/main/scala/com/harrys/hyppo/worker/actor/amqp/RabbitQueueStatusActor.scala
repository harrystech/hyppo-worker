package com.harrys.hyppo.worker.actor.amqp

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.config.WorkerConfig

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Created by jpetty on 9/16/15.
 */
object RabbitQueueStatusActor {
  //  Used to refresh the queue status information
  final case class QueueStatusUpdate(statuses: Seq[SingleQueueDetails])
  //  Used to provide partial updates about queue status after a dequeue event
  final case class PartialStatusUpdate(name: String, size: Int)

  trait Factory {
    def apply(@Assisted("delegator") delegator: ActorRef): Actor
  }
}

final class RabbitQueueStatusActor @Inject()
(
  config: WorkerConfig,
  naming: QueueNaming,
  httpClient: RabbitHttpClient,
  @Assisted("delegator") delegator: ActorRef
) extends Actor with ActorLogging {
  import RabbitQueueStatusActor._

  //  Bring the actor system threadpool into scope
  import context.dispatcher

  //  Establish a death-pact with the delegator
  context.watch(delegator)

  //  Used internally by a timer event to trigger refreshes of the queue status info
  private case object RefreshQueueStatsEvent

  //  Schedule a recurring refresh of the queue status info
  val statusTimer = context.system.scheduler.schedule(Duration.Zero, config.taskPollingInterval, self, RefreshQueueStatsEvent)

  override def postStop() : Unit = {
    statusTimer.cancel()
  }

  override def receive: Receive = {
    case RefreshQueueStatsEvent =>
      Future({
        val statuses = blocking {
          httpClient.fetchRawHyppoQueueDetails()
        }
        QueueStatusUpdate(statuses.filter(s => naming.isIntegrationQueueName(s.queueName)))
      }).onComplete({
        case Success(update) =>
          log.debug(s"Sending queue status update: ${ update }")
          delegator ! update
        case Failure(cause)  =>
          log.error(cause, "Failed to update queue status info")
      })

    case Terminated(dead) if dead.equals(delegator) =>
      log.debug("Shutting down")
      context.stop(self)
  }
}
