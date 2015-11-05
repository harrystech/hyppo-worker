package com.harrys.hyppo.worker.actor.amqp

import java.time.Duration
import java.util.regex.Pattern

import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.api.code.ExecutableIntegration

/**
  * Created by jpetty on 11/4/15.
  */
final class QueueNaming(config: HyppoConfig) {

  val resultsQueueName: String = s"$prefix.results"

  val generalQueueName: String = s"$prefix.general"

  val expiredQueueName: String = s"$prefix.expired"

  private val integrationPrefix: String = s"$prefix.integration"
  def integrationQueueName(integration: ExecutableIntegration) : String = {
    val sourceFix = sanitizeName(integration.sourceName)
    val version   = s"version-${integration.details.versionNumber}"
    s"$integrationPrefix.$sourceFix.$version"
  }

  def isIntegrationQueueName(name: String) : Boolean = {
    name.startsWith(integrationPrefix)
  }

  private val concurrencyResourcePrefix: String = s"$prefix.resource.concurrency"
  def concurrencyResource(resourceName: String, concurrency: Int) : WorkerResources.ConcurrencyWorkerResource = {
    if (concurrency <= 0){
      throw new IllegalArgumentException(s"Concurrency resources must have a concurrency value of 1 or more. Provided: $concurrency")
    }
    val nameFix   = sanitizeName(resourceName)
    val queueName = s"$concurrencyResourcePrefix.$nameFix-$concurrency"
    WorkerResources.ConcurrencyWorkerResource(resourceName, queueName, concurrency)
  }

  private val throttledResourcePrefix: String = s"$prefix.resource.throttled"
  def throttledResource(resourceName: String, throttle: Duration) : WorkerResources.ThrottledWorkerResource = {
    val nameFix   = sanitizeName(resourceName)
    val deferred  = s"$throttledResourcePrefix.defer.$nameFix"
    val available = s"$throttledResourcePrefix.ready.$nameFix"
    WorkerResources.ThrottledWorkerResource(resourceName, deferred, available, throttle)
  }

  private val cleanupPattern: Pattern = Pattern.compile("\\s")
  private def sanitizeName(input: String) : String = {
    cleanupPattern.matcher(input).replaceAll("_")
  }

  private def prefix: String = config.workQueuePrefix
}
