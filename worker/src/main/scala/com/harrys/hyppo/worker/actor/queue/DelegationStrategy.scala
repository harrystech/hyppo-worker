package com.harrys.hyppo.worker.actor.queue

import java.time.{Duration, Instant}
import javax.inject.Inject

import com.google.inject.ImplementedBy
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp.{SingleQueueDetails, QueueNaming}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.WorkResource
import com.harrys.hyppo.worker.scheduling.{ResourceQueueMetrics, Sigmoid, WorkQueueMetrics, WorkQueuePrioritizer}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Created by jpetty on 2/12/16.
  */
@ImplementedBy(classOf[DefaultDelegationStrategy])
trait DelegationStrategy {
  def priorityOrderWithoutAffinity(general: WorkQueueMetrics, integration: Seq[WorkQueueMetrics]): Iterator[SingleQueueDetails]
  def priorityOrderWithPreference(prefer: ExecutableIntegration, general: WorkQueueMetrics, integrations: Seq[WorkQueueMetrics]): Iterator[SingleQueueDetails]
}

final class DefaultDelegationStrategy @Inject()
(
  config:          WorkerConfig,
  naming:          QueueNaming,
  workPrioritizer: WorkQueuePrioritizer
) extends DelegationStrategy {

  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  override def priorityOrderWithoutAffinity(general: WorkQueueMetrics, integrations: Seq[WorkQueueMetrics]): Iterator[SingleQueueDetails] = {
    filterAndPrioritize(Seq(general) ++ integrations)
  }

  override def priorityOrderWithPreference(prefer: ExecutableIntegration, general: WorkQueueMetrics, integrations: Seq[WorkQueueMetrics]): Iterator[SingleQueueDetails] = {
    val isAffinityMatch    = naming.belongsToIntegration(prefer) _
    val (affinity, others) = integrations.partition(metrics => isAffinityMatch(metrics.details.queueName))

    var result = filterAndPrioritize(affinity)
    if (general.hasWork) {
      result ++= Seq(general.details)
    }
    result ++ filterAndPrioritize(others)
  }

  private def filterAndPrioritize(input: Seq[WorkQueueMetrics]): Iterator[SingleQueueDetails] = {
    val available = filterForResourceContention(input.filter(_.hasWork))
    workPrioritizer.prioritize(available.map(_.details))
  }

  private def filterForResourceContention(input: Seq[WorkQueueMetrics]): Seq[WorkQueueMetrics] = {
    val isAvailableResource = new StatefulResourceQueueFilter()
    input.filter(isAvailableResource)
  }

  /**
    * Used to internally track which [[WorkResource]] instances have been randomly omitted or included in the output
    * set so that multiple queues sharing a resource don't receive different randomized penalties
    */
  private final class StatefulResourceQueueFilter extends Function[WorkQueueMetrics, Boolean] {
    private var attempt = Set[WorkResource]()
    private var ignore  = Set[WorkResource]()

    override def apply(metrics: WorkQueueMetrics): Boolean = {
      val allowed = metrics.resources.forall(shouldAllowResource)
      if (!allowed) {
        log.debug(s"Rejecting work for queue ${ metrics.details.queueName } based on WorkResource contention")
      }
      allowed
    }

    private def shouldAllowResource(metrics: ResourceQueueMetrics): Boolean = metrics.timeOfLastContention match {
      case None => true
      case Some(time) if attempt.contains(metrics.resource) => true
      case Some(time) if ignore.contains(metrics.resource)  => false
      case Some(time)  =>
        val threshold = computeAllowanceThreshold(time)
        val randomVal = Random.nextDouble()
        if (randomVal <= threshold) {
          log.debug(s"Accepting work dependent on ${ metrics.resource } based on probabilistic backoff. Threshold $threshold >= $randomVal")
          attempt += metrics.resource
          true
        } else {
          log.debug(s"Ignoring work dependent on ${ metrics.resource } based on probabilistic backoff. Threshold $threshold < $randomVal")
          ignore += metrics.resource
          false
        }
    }

    private def computeAllowanceThreshold(timeOfContention: Instant): Double = {
      val seconds = Duration.between(timeOfContention, Instant.now()).getSeconds.toInt
      Sigmoid.gompertzCurveBackoffFactor(seconds, config.resourceBackoffFactor, config.resourceBackoffMinValue)
    }
  }
}

