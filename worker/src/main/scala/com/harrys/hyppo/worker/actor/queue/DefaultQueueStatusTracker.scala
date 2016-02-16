package com.harrys.hyppo.worker.actor.queue

import javax.inject.Inject

import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.RabbitQueueStatusActor.{PartialStatusUpdate, QueueStatusUpdate}
import com.harrys.hyppo.worker.actor.amqp._
import com.harrys.hyppo.worker.api.proto.{ThrottledWorkResource, ConcurrencyWorkResource, WorkResource}
import com.harrys.hyppo.worker.scheduling.{ResourceQueueMetrics, WorkQueueMetrics}

/**
  * Created by jpetty on 2/12/16.
  */
final class DefaultQueueStatusTracker @Inject()(naming: QueueNaming, helpers: QueueHelpers) extends QueueStatusTracker {

  private var queues    = Map[String, TrackingRecord]()

  private var resources = Map[String, WorkResource]()

  private final case class TrackingRecord(details: SingleQueueDetails, resources: Seq[WorkResource]) {
    def hasResources: Boolean = resources.nonEmpty
    def resourceNames: Seq[String] = resources.map(_.resourceName)
    def hasWorkInQueue: Boolean = details.ready > 0
  }


  override def integrationQueueMetrics(): Seq[WorkQueueMetrics] = {
    val integrations = queues.values.toIndexedSeq.filter { tracking => naming.isIntegrationQueueName(tracking.details.queueName) }
    integrations.map { tracking =>
      val resources  = tracking.resources.flatMap { resource => fetchWorkResourceDetails(resource) }
      WorkQueueMetrics(tracking.details, resources = resources)
    }
  }

  override def handleStatusUpdate(update: QueueStatusUpdate): Unit = {
    val hyppoQueues   = update.statuses.filter(details => naming.belongsToHyppo(details.queueName))
    val withResources = hyppoQueues.map { detail =>
      val name = detail.queueName
      if (queues.contains(name)) {
        name -> TrackingRecord(detail, queues(name).resources)
      } else {
        name -> TrackingRecord(detail, Seq())
      }
    }
    queues = withResources.toMap
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


  private def updateWithDetails(name: String, details: SingleQueueDetails): Unit = {
    if (queues.contains(name)) {
      val record = queues(name)
      queues += name -> TrackingRecord(details = details, record.resources)
    } else {
      queues += name -> TrackingRecord(details = details, resources = Seq())
    }
  }

  /**
    * Extracts known queue information for a given [[WorkResource]]. The underlying queue representations of
    * a [[ConcurrencyWorkResource]] is only a single queue while [[ThrottledWorkResource]] instances have two
    * underlying queues.
    * @param resource The [[WorkResource]] to fetch information on.
    * @return The [[ResourceQueueMetrics]] extracted for the provided resource, if it exists.
    */
  private def fetchWorkResourceDetails(resource: WorkResource): Option[ResourceQueueMetrics] = resource match {
    case c: ConcurrencyWorkResource =>
      queues.get(c.queueName).map { tracking => ResourceQueueMetrics(resource, tracking.details) }
    case t: ThrottledWorkResource   =>
      if (queues.contains(t.availableQueueName) && queues.contains(t.deferredQueueName)) {
        val available = queues(t.availableQueueName).details
        val deferred  = queues(t.deferredQueueName).details
        Some(ResourceQueueMetrics(resource, MultiQueueDetails(Seq(available, deferred))))
      } else {
        None
      }
  }

  override def resourcesAcquiredSuccessfully(queue: String, resources: Seq[WorkResource]): Unit = {
    registerResourcesToQueue(queue, resources)
    //  TODO: WorkResource penalty calculations here
    ???
  }

  override def resourceAcquisitionFailed(queue: String, resources: Seq[WorkResource], failure: WorkResource): Unit = {
    registerResourcesToQueue(queue, resources)
    //  TODO: WorkResource penalty calculations here
    ???
  }

  private def registerResourcesToQueue(queue: String, resources: Seq[WorkResource]): Unit = {
    if (queues.contains(queue)){
      val tracking = queues(queue)
      if (!tracking.hasResources) {
        queues += queue -> tracking.copy(resources = resources)
      }
    } else {
      val details = SingleQueueDetails(queue, size = 0, rate = 0.0, ready = 0, unacknowledged = 0, idleSince = TimeUtils.currentLocalDateTime())
      queues += queue -> TrackingRecord(details, resources)
    }
  }
}
