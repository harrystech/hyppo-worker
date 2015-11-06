package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, QueueNaming}
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation.{ResourceUnavailable, ResourceAcquisitionResult, AcquiredResourceLeases}
import com.harrys.hyppo.worker.actor.sync.ThrottledResourceLease
import com.harrys.hyppo.worker.api.proto.{ThrottledWorkResource, ConcurrencyWorkResource, WorkResource}
import com.rabbitmq.client.Channel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jpetty on 11/6/15.
  */
final class ResourceManagement {

  def leaseResources(channel: Channel, resources: Seq[WorkResource]) : Either[AcquiredResourceLeases, ResourceUnavailable] = {
    val result: Either[AcquiredResourceLeases, ResourceUnavailable] = Left(AcquiredResourceLeases(Seq()))
    resourceAcquisitionOrder(resources).foldLeft(result) { (previous, resource) =>
      previous.left.flatMap { acquired =>
        leaseResource(channel, resource) match {
          case Some(lease) =>
            Left(AcquiredResourceLeases(acquired.leases :+ lease))
          case None =>
            //  Rollback all previously acquired resources
            releaseResources(channel, acquired)
            Right(ResourceUnavailable(resource))
        }
      }
    }
  }

  def releaseResources(channel: Channel, resources: AcquiredResourceLeases) : Unit = {
    resources.leases.foreach(r => releaseResource(channel, r))
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
      Some(ConcurrencyResourceLease(resource, response))
    }
  }

  private def leaseThrottledResource(channel: Channel, resource: ThrottledWorkResource) : Option[ThrottledResourceLease] = {
    val response = channel.basicGet(resource.availableQueueName, false)
    if (response == null){
      None
    } else {
      Some(ThrottledResourceLease(resource, response))
    }
  }

  private def releaseResource(channel: Channel, lease: ResourceLease) : Unit = lease match {
    case c: ConcurrencyResourceLease => releaseConcurrencyResource(channel, c)
    case t: ThrottledResourceLease   => releaseThrottledResource(channel, t)
  }

  private def releaseConcurrencyResource(channel: Channel, lease: ConcurrencyResourceLease) : Unit = {
    channel.basicReject(lease.deliveryTag, true)
  }

  private def releaseThrottledResource(channel: Channel, lease: ThrottledResourceLease) : Unit = {
    val props = AMQPMessageProperties.throttleTokenProperties(lease.resource)
    val body  = Array[Byte]()
    channel.txSelect()
    channel.basicAck(lease.deliveryTag, false)
    channel.basicPublish("", lease.resource.deferredQueueName, true, false, props, body)
    channel.txCommit()
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
