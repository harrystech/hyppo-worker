package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, ThrottledWorkResource, WorkResource}
import com.rabbitmq.client.Channel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jpetty on 11/6/15.
  */
final class ResourceLeasing {

  def leaseResources(channel: Channel, resources: Seq[WorkResource]) : Either[AcquiredResourceLeases, ResourceUnavailable] = {
    val result: Either[AcquiredResourceLeases, ResourceUnavailable] = Left(AcquiredResourceLeases(Seq()))
    resourceAcquisitionOrder(resources).foldLeft(result) { (previous, resource) =>
      previous.left.flatMap { acquired =>
        leaseResource(channel, resource) match {
          case Some(lease) =>
            Left(AcquiredResourceLeases(acquired.leases :+ lease))
          case None =>
            //  Rollback all previously acquired resources
            acquired.releaseAllUnconsumed()
            Right(ResourceUnavailable(resource))
        }
      }
    }
  }

  private def leaseResource(channel: Channel, request: WorkResource) : Option[ResourceLease] = request match {
    case c: ConcurrencyWorkResource => leaseConcurrencyResource(channel, c)
    case t: ThrottledWorkResource   => leaseThrottledResource(channel, t)
  }

  private def leaseConcurrencyResource(channel: Channel, resource: ConcurrencyWorkResource) : Option[ConcurrencyResourceLease] = {
    val response = channel.basicGet(resource.queueName, false)
    if (response == null) {
      None
    } else {
      Some(ConcurrencyResourceLease(resource, channel, response))
    }
  }

  private def leaseThrottledResource(channel: Channel, resource: ThrottledWorkResource) : Option[ThrottledResourceLease] = {
    val response = channel.basicGet(resource.availableQueueName, false)
    if (response == null){
      None
    } else {
      Some(ThrottledResourceLease(resource, channel, response))
    }
  }

  /**
    * Sorts the resources into the ordering that they will be acquired in
    * @param resources The sequence of [[WorkResource]] instances to be sorted
    * @return The ordering to use when attempting to acquire resources
    */
  def resourceAcquisitionOrder(resources: Seq[WorkResource]) : Seq[WorkResource] = {
    val concurrency = ArrayBuffer[ConcurrencyWorkResource]()
    val throttling  = ArrayBuffer[ThrottledWorkResource]()
    resources.collect {
      case c: ConcurrencyWorkResource => concurrency.append(c)
      case t: ThrottledWorkResource   => throttling.append(t)
    }
    (concurrency.sortBy(_.resourceName) ++ throttling.sortBy(_.resourceName)).toSeq
  }
}
