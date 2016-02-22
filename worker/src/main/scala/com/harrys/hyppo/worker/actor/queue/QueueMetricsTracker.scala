package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import com.google.inject.ImplementedBy
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{PartialStatusUpdate, QueueStatusUpdate}
import com.harrys.hyppo.worker.actor.amqp.{MultiQueueDetails, QueueHelpers, QueueNaming, SingleQueueDetails}
import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, ThrottledWorkResource, WorkResource}
import com.harrys.hyppo.worker.scheduling.{RecentResourceContention, ResourceQueueMetrics, WorkQueueMetrics}

/**
  * Created by jpetty on 2/12/16.
  */
@ImplementedBy(classOf[DefaultQueueMetricsTracker])
trait QueueMetricsTracker {
  def generalQueueMetrics(): WorkQueueMetrics
  def integrationQueueMetrics(): Seq[WorkQueueMetrics]

  def handleStatusUpdate(update: QueueStatusUpdate): Unit
  def handleStatusUpdate(update: PartialStatusUpdate): Unit

  def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit
  def resourceAcquisitionFailed(queue: String, resources: Seq[WorkResource], failure: WorkResource): Unit
}

final class DefaultQueueMetricsTracker @Inject()
(
  naming: QueueNaming,
  helpers: QueueHelpers,
  failureTiming: RecentResourceContention
) extends QueueMetricsTracker {

  private var queues = Map[String, WorkQueueTracking]()

  private final case class WorkQueueTracking(details: SingleQueueDetails, resources: Seq[WorkResource]) {
    def hasResources: Boolean = resources.nonEmpty
    def resourceNames: Seq[String] = resources.map(_.resourceName)
    def hasWorkInQueue: Boolean = details.ready > 0
  }

  override def generalQueueMetrics(): WorkQueueMetrics = {
    WorkQueueMetrics(trackingForName(naming.generalQueueName).details, Seq())
  }

  override def integrationQueueMetrics(): Seq[WorkQueueMetrics] = {
    val integrations = queues.values.filter { tracking =>
      naming.isIntegrationQueueName(tracking.details.queueName)
    }.toIndexedSeq

    integrations.map { tracking =>
      val resources = tracking.resources.flatMap { resource => fetchWorkResourceDetails(resource) }
      WorkQueueMetrics(tracking.details, resources = resources)
    }
  }

  override def handleStatusUpdate(update: QueueStatusUpdate): Unit = {
    var resourceSet   = Set[WorkResource]()
    val hyppoQueues   = update.statuses.filter(details => naming.belongsToHyppo(details.queueName))
    val withResources = hyppoQueues.map { detail =>
      val name = detail.queueName
      if (queues.contains(name)) {
        val resources = queues(name).resources
        resourceSet ++= resources
        name -> WorkQueueTracking(detail, resources)
      } else {
        name -> WorkQueueTracking(detail, Seq())
      }
    }
    queues = withResources.toMap
    failureTiming.resetContents(resourceSet)
  }

  override def handleStatusUpdate(update: PartialStatusUpdate): Unit = {
    if (naming.belongsToHyppo(update.name)) {
      if (queues.contains(update.name)) {
        val details = queues(update.name).details
        updateWithDetails(update.name, details.copy(size = update.size))
      } else {
        val details = SingleQueueDetails(update.name, update.size, rate = 0.0, ready = update.size, unacknowledged = 0, TimeUtils.currentLocalDateTime())
        updateWithDetails(update.name, details)
      }
    }
  }

  override def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit = {
    registerResourcesToQueue(queue, resources)
    failureTiming.successfullyAcquired(resources)
  }

  override def resourceAcquisitionFailed(queue: String, resources: Seq[WorkResource], failure: WorkResource): Unit = {
    registerResourcesToQueue(queue, resources)
    failureTiming.failedToAcquire(failure)
  }

  private def trackingForName(name: String): WorkQueueTracking = {
    queues.getOrElse(name, WorkQueueTracking(unknownQueueDetails(name), Seq()))
  }

  private def unknownQueueDetails(name: String): SingleQueueDetails = {
    unknownQueueDetails(name, 0)
  }

  private def unknownQueueDetails(name: String, size: Int): SingleQueueDetails = {
    SingleQueueDetails(name, size = size, rate = 0.0, ready = 0, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime())
  }

  private def updateWithDetails(name: String, details: SingleQueueDetails): Unit = {
    if (queues.contains(name)) {
      val record = queues(name)
      queues += name -> WorkQueueTracking(details = details, record.resources)
    } else {
      queues += name -> WorkQueueTracking(details = details, resources = Seq())
    }
  }

  /**
    * Extracts known queue information for a given [[WorkResource]]. The underlying queue representations of
    * a [[ConcurrencyWorkResource]] is only a single queue while [[ThrottledWorkResource]] instances have two
    * underlying queues.
    *
    * @param resource The [[WorkResource]] to fetch information on.
    * @return The [[ResourceQueueMetrics]] extracted for the provided resource, if it exists.
    */
  private def fetchWorkResourceDetails(resource: WorkResource): Option[ResourceQueueMetrics] = resource match {
    case c: ConcurrencyWorkResource =>
      queues.get(c.queueName).map { tracking =>
        ResourceQueueMetrics(resource, tracking.details, failureTiming.timeOfLastContention(resource))
      }
    case t: ThrottledWorkResource =>
      if (queues.contains(t.availableQueueName) && queues.contains(t.deferredQueueName)) {
        val available = queues(t.availableQueueName).details
        val deferred  = queues(t.deferredQueueName).details
        Some(ResourceQueueMetrics(resource, MultiQueueDetails(Seq(available, deferred)), failureTiming.timeOfLastContention(resource)))
      } else {
        None
      }
  }

  private def registerResourcesToQueue(queue: String, resources: Seq[WorkResource]): Unit = {
    if (queues.contains(queue)) {
      val tracking = queues(queue)
      if (!tracking.hasResources) {
        queues += queue -> tracking.copy(resources = resources)
      }
    } else {
      queues += queue -> WorkQueueTracking(unknownQueueDetails(queue), resources)
    }
  }
}