package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import com.google.inject.ImplementedBy
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{PartialStatusUpdate, QueueStatusUpdate}
import com.harrys.hyppo.worker.actor.amqp.{MultiQueueDetails, QueueHelpers, QueueNaming, SingleQueueDetails}
import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, ThrottledWorkResource, WorkResource}
import com.harrys.hyppo.worker.scheduling.{RecentResourceContention, ResourceQueueMetrics, WorkQueueMetrics}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

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
  naming:         QueueNaming,
  failureTiming:  RecentResourceContention
) extends QueueMetricsTracker {

  private val log = Logger(LoggerFactory.getLogger(this.getClass))

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
    val integrations = queues.values.view.filter(tracking => naming.isIntegrationQueueName(tracking.details.queueName))
    val withMetrics  = integrations.map { tracking =>
      val resources = tracking.resources.flatMap(fetchWorkResourceDetails)
      WorkQueueMetrics(tracking.details, resources = resources)
    }
    withMetrics.toIndexedSeq
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
    if (queues.contains(update.name)) {
      val details = queues(update.name).details
      if (details.size != update.size) {
        val ready = Math.min(update.size, details.ready)
        val unack = Math.min(update.size, details.unacknowledged)
        updateWithDetails(update.name, details.copy(size = update.size, ready = ready, unacknowledged = unack))
      }
    } else {
      val details = unknownQueueDetails(update.name, update.size)
      updateWithDetails(update.name, details)
    }
  }

  override def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit = {
    if (resources.nonEmpty) {
      registerResourcesToQueue(queue, resources)
      failureTiming.successfullyAcquired(resources)
    }
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
      if (tracking.resources != resources) {
        log.debug(s"Discovered resources required for queue $queue: ${ resources.map(_.resourceName).mkString(", ") }")
        queues += queue -> tracking.copy(resources = resources)
        resources.foreach(createLocalResourceTracking)
      }
    } else if (resources.nonEmpty) {
      log.debug(s"Discovered resources required for queue $queue: ${ resources.map(_.resourceName).mkString(", ") }")
      queues += queue -> WorkQueueTracking(unknownQueueDetails(queue), resources)
      resources.foreach(createLocalResourceTracking)
    }
  }

  private def createLocalResourceTracking(resource: WorkResource): Unit = resource match {
    case c: ConcurrencyWorkResource =>
      val name = c.queueName
      if (!queues.contains(name)) {
        log.debug(s"Creating stub tracking entry for previously unknown resource: ${ c.inspect }")
        queues += name -> WorkQueueTracking(details = unknownQueueDetails(name, c.concurrency), resources = Seq())
      }
    case t: ThrottledWorkResource =>
      val available = t.availableQueueName
      val deferred  = t.deferredQueueName
      if (!queues.contains(available) && !queues.contains(deferred)) {
        log.debug(s"Creating stub tracking entry for previously unknown resource: ${t.inspect}")
        val stubs = Seq[(String, WorkQueueTracking)](
          available -> WorkQueueTracking(details = unknownQueueDetails(available, 1), resources = Seq()),
          deferred  -> WorkQueueTracking(details = unknownQueueDetails(deferred, 0), resources = Seq())
        )
        queues ++= stubs
      }
  }
}