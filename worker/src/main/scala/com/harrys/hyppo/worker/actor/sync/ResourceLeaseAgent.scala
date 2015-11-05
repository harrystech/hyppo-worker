package com.harrys.hyppo.worker.actor.sync

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.actor.amqp.Resources._
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation._
import com.rabbitmq.client.AMQP
import com.thenewmotion.akka.rabbitmq._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jpetty on 11/3/15.
 */
final class ResourceLeaseAgent(config: HyppoConfig, connection: ActorRef) extends Actor with ActorLogging  {

  val channel = context.watch(connection.createChannel(ChannelActor.props(), name = Some("resource-leases")))

  override def receive: Receive = {
    case RequestForResources(resources) =>
      val leasingActor = sender()
      val acquireOrder = resourceAcquisitionOrder(resources)
      performResourceAcquisition(leasingActor, acquireOrder)

    case ReleaseResources(resources) =>
      releaseResources(resources)
  }

  def performResourceAcquisition(leasingActor: ActorRef, resources: Seq[Resource]) : Unit = {
    if (resources.isEmpty){
      leasingActor ! AcquiredResourceLeases(Seq())
    } else {
      channel ! ChannelMessage(c => performResourceAcquisition(c, leasingActor, resources), dropIfNoChannel = false)
    }
  }

  def performResourceAcquisition(channel: Channel, leasingActor: ActorRef, resources: Seq[Resource]) : Unit = {
    val emptyLeases: ResourceAcquisitionResult = AcquiredResourceLeases(Seq())
    if (resources.isEmpty){
      leasingActor ! emptyLeases
    } else {
      resources.foldLeft(emptyLeases) { (result, resource) =>
        result match {
          case u: ResourceUnavailable    => u
          case a: AcquiredResourceLeases =>
            leaseResourceOrRollback(channel, a, resource)
        }
      }
    }
  }

  def leaseResourceOrRollback(channel: Channel, acquired: AcquiredResourceLeases, attempt: Resource) : ResourceAcquisitionResult = {
    try {
      leaseResource(channel, attempt) match {
        case Some(lease) =>
          log.debug(s"Successfully acquired resource: ${attempt.resourceName}")
          AcquiredResourceLeases(acquired.leases :+ lease)
        case None =>
          log.debug(s"Rolling back resource leases due to unavailable resource: ${attempt.resourceName}")
          releaseResources(acquired.leases)
          ResourceUnavailable(attempt)
      }
    } catch {
      case t: Throwable =>
        log.error(t, s"Rolling back resource leases due to exception while acquiring resource: ${attempt.resourceName}")
        releaseResources(acquired.leases)
        ResourceUnavailable(attempt)
    }
  }

  def leaseResource(channel: Channel, request: Resource) : Option[ResourceLease] = request match {
    case c: ConcurrencyResource => leaseConcurrencyResource(channel, c)
    case t: ThrottledResource   => leaseThrottledResource(channel, t)
  }

  def leaseConcurrencyResource(channel: Channel, resource: ConcurrencyResource) : Option[ConcurrencyResourceLease] = {
    val response = channel.basicGet(resource.queueName, false)
    if (response == null) {
      log.debug(s"No available resource tokens in queue: ${resource.resourceName}")
      None
    } else {
      log.debug(s"Successfully acquired resource token from: ${resource.resourceName}")
      Some(ConcurrencyResourceLease(resource, channel, response))
    }
  }

  def releaseResources(release: Seq[ResourceLease]) : Unit = {
    channel ! ChannelMessage(c => releaseResources(c, release), dropIfNoChannel = false)
  }

  def releaseResources(channel: Channel, rollback: Seq[ResourceLease]) : Unit = {
    try {
      rollback.collect {
        case c: ConcurrencyResourceLease => releaseConcurrencyResource(channel, c)
        case t: ThrottledResourceLease   => releaseThrottledResource(channel, t)
      }
    } catch {
      case e: Exception =>
        log.error(e, "Failure while releasing resources! Force closing the channel to reset the lease tokens")
        channel.close(AMQP.RESOURCE_ERROR, "Failed to release resource tokens from queues!")
        throw e
    }
  }

  def releaseConcurrencyResource(channel: Channel, lease: ConcurrencyResourceLease) : Unit = {
    if (channel.getChannelNumber == lease.channel.getChannelNumber){
      log.debug(s"Releasing concurrency lease on resource: ${ lease.resourceName }")
      channel.basicReject(lease.deliveryTag, true)
    } else {
      log.error(s"${lease.toString} found being released on unexpected channel! Found channel ${channel.getChannelNumber} instead of expected channel ${lease.channel.getChannelNumber}")
    }
  }

  def releaseThrottledResource(channel: Channel, lease: ThrottledResourceLease) : Unit = {
    if (channel.getChannelNumber == lease.channel.getChannelNumber){
      log.debug(s"Releasing throttled lease on resource: ${ lease.resourceName }")
      throw new NotImplementedError("Throttled leases are not yet fully supported!")
    } else {
      log.error(s"${lease.toString} found being released on unexpected channel! Found channel ${channel.getChannelNumber} instead of expected channel ${lease.channel.getChannelNumber}")
    }
  }

  /**
   * Sorts the resources into the ordering that they will be acquired in
   * @param resources The sequence of [[Resource]] instances to be sorted
   * @return The ordering to use when attempting to acquire resources
   */
  def resourceAcquisitionOrder(resources: Seq[Resource]) : Seq[Resource] = {
    val concurrency = ArrayBuffer[Resource]()
    val throttling  = ArrayBuffer[Resource]()
    resources.collect {
      case c: ConcurrencyResource => concurrency.append(c)
      case t: ThrottledResource   => throttling.append(t)
    }
    (concurrency ++ throttling).toSeq
  }


  def leaseThrottledResource(channel: Channel, request: ThrottledResource) : Option[ThrottledResourceLease] = {
    throw new Exception("NOT IMPLEMENTED")
  }
}

