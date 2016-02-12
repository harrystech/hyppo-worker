package com.harrys.hyppo.worker.actor.amqp

import java.security.MessageDigest
import java.time.Duration
import java.util.regex.Pattern

import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.actor.queue.ResourceLeasing
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, IntegrationWorkerInput, ThrottledWorkResource, WorkResource}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.Charsets

/**
  * Created by jpetty on 11/4/15.
  */
final class QueueNaming(config: HyppoConfig) {

  private val prefix: String = config.workQueuePrefix

  val resultsQueueName: String = s"$prefix.results"

  val generalQueueName: String = s"$prefix.general"

  val expiredQueueName: String = s"$prefix.expired"

  def belongsToHyppo(queueName: String) : Boolean = {
    queueName.startsWith(prefix)
  }

  def integrationWorkQueueName(input: IntegrationWorkerInput) : String = {
    integrationWorkQueueName(input.integration, input.resources)
  }

  def integrationWorkQueueName(integration: ExecutableIntegration, resources: Seq[WorkResource]) : String = {
    val base = integrationQueueBaseName(integration)
    if (resources.isEmpty){
      base
    } else {
      s"$base.${resourceUniqueSuffix(resources)}"
    }
  }

  private val integrationPrefix: String = s"$prefix.integration"
  private def integrationQueueBaseName(integration: ExecutableIntegration): String = {
    val sourceFix = sanitizeIntegrationName(integration.sourceName)
    val version   = s"v-${integration.details.versionNumber}"
    s"$integrationPrefix.$sourceFix-$version"
  }

  def isIntegrationQueueName(name: String) : Boolean = {
    name.startsWith(integrationPrefix)
  }

  private val logicalBaseRegex = s"""$integrationPrefix\\.([^\\.]+).*""".r
  def toLogicalQueueDetails(details: Iterable[SingleQueueDetails]): Seq[QueueDetails] = {
    val valueGroups = details.groupBy { single =>
      single.queueName match {
        case logicalBaseRegex(group) => group
        case unmatched => unmatched
      }
    }.values
    valueGroups.toIndexedSeq.map { group =>
      val seq = group.toIndexedSeq
      if (seq.size == 1){
        seq.head
      } else {
        MultiQueueDetails(seq)
      }
    }
  }

  def filterForIntegration(integration: ExecutableIntegration, queues: Iterable[String]) : Iterable[String] = {
    val filter = belongsToIntegration(integration) _
    queues.filter(filter)
  }

  def belongsToIntegration(integration: ExecutableIntegration)(toCheck: String) : Boolean = {
    val prefix = integrationQueueBaseName(integration)
    toCheck.startsWith(prefix)
  }

  private val concurrencyResourcePrefix: String = s"$prefix.resource.concurrency"
  def concurrencyResource(resourceName: String, concurrency: Int) : ConcurrencyWorkResource = {
    if (concurrency <= 0){
      throw new IllegalArgumentException(s"Concurrency resources must have a concurrency value of 1 or more. Provided: $concurrency")
    }
    val nameFix   = sanitizeIntegrationName(resourceName)
    val queueName = s"$concurrencyResourcePrefix.$nameFix-$concurrency"
    ConcurrencyWorkResource(resourceName, queueName, concurrency)
  }

  private val throttledResourcePrefix: String = s"$prefix.resource.throttled"
  def throttledResource(resourceName: String, throttle: Duration) : ThrottledWorkResource = {
    val nameFix   = sanitizeIntegrationName(resourceName)
    val deferred  = s"$throttledResourcePrefix.defer.$nameFix"
    val available = s"$throttledResourcePrefix.ready.$nameFix"
    ThrottledWorkResource(resourceName, deferred, available, throttle)
  }

  private val whitespace: Pattern = Pattern.compile("\\s")
  private val dotValues:  Pattern = Pattern.compile("\\.")
  private def sanitizeIntegrationName(input: String) : String = {
    val noWhiteSpace = whitespace.matcher(input).replaceAll("_")
    dotValues.matcher(noWhiteSpace).replaceAll("-")
  }

  private val resourceManagement = new ResourceLeasing
  private def resourceUniqueSuffix(resources: Seq[WorkResource]) : String = {
    if (resources.isEmpty){
      ""
    } else {
      val digest = MessageDigest.getInstance("MD5")
      resourceManagement.resourceAcquisitionOrder(resources).foreach { resource =>
        digest.update(resource.getClass.getName.getBytes(Charsets.UTF_8))
        digest.update(resource.resourceName.getBytes(Charsets.UTF_8))
      }
      Hex.encodeHexString(digest.digest()).substring(0, 8)
    }
  }
}
