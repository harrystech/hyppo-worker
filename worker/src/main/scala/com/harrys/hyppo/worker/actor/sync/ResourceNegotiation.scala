package com.harrys.hyppo.worker.actor.sync

import com.harrys.hyppo.worker.actor.amqp.Resources._


/**
 * Created by jpetty on 11/3/15.
 */
object ResourceNegotiation {

  final case class RequestForResources(resources: Seq[Resource])

  final case class ReleaseResources(resources: Seq[ResourceLease])

  sealed trait ResourceAcquisitionResult

  final case class AcquiredResourceLeases(leases: Seq[ResourceLease]) extends ResourceAcquisitionResult

  final case class ResourceUnavailable(unavailable: Resource) extends ResourceAcquisitionResult {
    def resourceName: String = unavailable.resourceName
  }

}
