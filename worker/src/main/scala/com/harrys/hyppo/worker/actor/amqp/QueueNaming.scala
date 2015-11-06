package com.harrys.hyppo.worker.actor.amqp

import java.security.MessageDigest
import java.time.Duration
import java.util.regex.Pattern

import akka.util.HashCode
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.actor.queue.ResourceManagement
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.{WorkResource, IntegrationWorkerInput, ConcurrencyWorkResource, ThrottledWorkResource}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.Charsets

/**
  * Created by jpetty on 11/4/15.
  */
final class QueueNaming(config: HyppoConfig) {

  val resultsQueueName: String = s"$prefix.results"

  val generalQueueName: String = s"$prefix.general"

  val expiredQueueName: String = s"$prefix.expired"

  def integrationWorkQueueName(input: IntegrationWorkerInput) : String = {
    val base = integrationQueueBaseName(input.integration)
    if (input.resources.isEmpty){
      base
    } else {
      s"$base.${resourceUniqueSuffix(input.resources)}"
    }
  }

  private val integrationPrefix: String = s"$prefix.integration"
  private def integrationQueueBaseName(integration: ExecutableIntegration) : String = {
    val sourceFix = sanitizeName(integration.sourceName)
    val version   = s"version-${integration.details.versionNumber}"
    s"$integrationPrefix.$sourceFix.$version"
  }

  def isIntegrationQueueName(name: String) : Boolean = {
    name.startsWith(integrationPrefix)
  }

  private val concurrencyResourcePrefix: String = s"$prefix.resource.concurrency"
  def concurrencyResource(resourceName: String, concurrency: Int) : ConcurrencyWorkResource = {
    if (concurrency <= 0){
      throw new IllegalArgumentException(s"Concurrency resources must have a concurrency value of 1 or more. Provided: $concurrency")
    }
    val nameFix   = sanitizeName(resourceName)
    val queueName = s"$concurrencyResourcePrefix.$nameFix-$concurrency"
    ConcurrencyWorkResource(resourceName, queueName, concurrency)
  }

  private val throttledResourcePrefix: String = s"$prefix.resource.throttled"
  def throttledResource(resourceName: String, throttle: Duration) : ThrottledWorkResource = {
    val nameFix   = sanitizeName(resourceName)
    val deferred  = s"$throttledResourcePrefix.defer.$nameFix"
    val available = s"$throttledResourcePrefix.ready.$nameFix"
    ThrottledWorkResource(resourceName, deferred, available, throttle)
  }

  private val cleanupPattern: Pattern = Pattern.compile("\\s")
  private def sanitizeName(input: String) : String = {
    cleanupPattern.matcher(input).replaceAll("_")
  }

  private val resourceManagement = new ResourceManagement
  private def resourceUniqueSuffix(resources: Seq[WorkResource]) : String = {
    if (resources.isEmpty){
      ""
    } else {
      val digest = MessageDigest.getInstance("MD5")
      resourceManagement.resourceAcquisitionOrder(resources).foreach {
        case c: ConcurrencyWorkResource =>
          digest.update('c')
          digest.update(c.resourceName.getBytes(Charsets.UTF_8))
        case t: ThrottledWorkResource =>
          digest.update('t')
          digest.update(t.resourceName.getBytes(Charsets.UTF_8))
      }
      Hex.encodeHexString(digest.digest()).substring(0, 8)
    }
  }

  private def prefix: String = config.workQueuePrefix
}
