package com.harrys.hyppo.worker.scheduling

import java.time.{Clock, Duration, Instant}

import com.harrys.hyppo.worker.api.proto.WorkResource

/**
  * Created by jpetty on 2/19/16.
  */
final class RecentResourceContention(retentionMax: Duration) {
  if (retentionMax.isNegative){
    throw new IllegalArgumentException(s"The maximum duration of contention is not allowed to be negative! Received: $retentionMax")
  }

  private var resourceContentionTiming = Map[WorkResource, Instant]()

  def failedToAcquire(resource: WorkResource): Unit = {
    resourceContentionTiming += resource -> currentInstant()
  }

  def successfullyAcquired(resources: Seq[WorkResource]): Unit = {
    resourceContentionTiming --= resources
  }

  def resetContents(resources: Set[WorkResource]): Unit = {
    val now = currentInstant()
    //  Expires any elements from the backing map that are no longer known resources or that have exceeded the
    //  max retention age.
    resourceContentionTiming = resourceContentionTiming.filter { pair =>
      resources.contains(pair._1) && Duration.between(pair._2, now).minus(retentionMax).isNegative
    }
  }

  def timeOfLastContention(resource: WorkResource): Option[Instant] = resourceContentionTiming.get(resource)

  private def currentInstant(): Instant = Instant.now(Clock.systemUTC())

}
