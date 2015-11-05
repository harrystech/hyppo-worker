package com.harrys.hyppo.worker.actor.sync

import akka.actor._
import com.harrys.hyppo.Lifecycle
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.actor.amqp.AMQPMessageProperties
import com.harrys.hyppo.worker.actor.amqp.WorkerResources._
import com.harrys.hyppo.worker.actor.sync.ResourceNegotiation._
import com.rabbitmq.client.{AMQP, ShutdownListener, ShutdownSignalException}
import com.thenewmotion.akka.rabbitmq._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jpetty on 11/3/15.
 */
final class ResourceLeaseAgent(config: HyppoConfig, connection: ActorRef) extends Actor with ActorLogging  {

  private final case class ChannelShutdown(channel: Channel, cause: ShutdownSignalException)

  val channelActor = context.watch(connection.createChannel(ChannelActor.props(setupChannel), name = Some("resource-leases")))

  def setupChannel(channel: Channel, self: ActorRef) : Unit = {
    log.info(s"Resource leasing agent received new channel: ${ channel.getChannelNumber }")
    channel.addShutdownListener(new ShutdownListener {
      override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
        self ! ChannelShutdown(channel, cause)
      }
    })
  }

  def shutdownImminent: Receive = {
    case ChannelShutdown(channel, cause) =>
      log.info(s"Channel ${channel.getChannelNumber} exited normally during shutdown: ${ cause.getMessage }")

    case Terminated(actor) if channelActor == actor =>
      log.info(s"Resource channel actor terminated successfully")
      context.stop(self)
  }

  override def receive: Receive = {
    case RequestForResources(resources) =>
      val leasingActor = sender()
      val acquireOrder = resourceAcquisitionOrder(resources)
      performResourceAcquisition(leasingActor, acquireOrder)

    case ReleaseResources(resources) =>
      releaseResources(resources)

    case ChannelShutdown(channel, cause) =>
      log.warning(s"Channel ${ channel.getChannelNumber } shutdown, releasing all resources. Cause: ${ cause.getMessage }")

    case Lifecycle.ImpendingShutdown =>
      log.info("Shutdown in progress. Terminating current resource channel")
      context.stop(channelActor)
      context.become(shutdownImminent)
  }

  def performResourceAcquisition(leasingActor: ActorRef, resources: Seq[WorkerResource]) : Unit = {
    if (resources.isEmpty){
      leasingActor ! AcquiredResourceLeases(Seq())
    } else {
      channelActor ! ChannelMessage(c => performResourceAcquisition(c, leasingActor, resources), dropIfNoChannel = false)
    }
  }

  def performResourceAcquisition(channel: Channel, leasingActor: ActorRef, resources: Seq[WorkerResource]) : Unit = {
    val emptyLeases: ResourceAcquisitionResult = AcquiredResourceLeases(Seq())
    if (resources.isEmpty){
      leasingActor ! emptyLeases
    } else {
      val leases = resources.foldLeft(emptyLeases) { (result, resource) =>
        result match {
          case u: ResourceUnavailable    => u
          case a: AcquiredResourceLeases =>
            leaseResourceOrRollback(channel, a, resource)
        }
      }
      leasingActor ! leases
    }
  }

  def leaseResourceOrRollback(channel: Channel, acquired: AcquiredResourceLeases, attempt: WorkerResource) : ResourceAcquisitionResult = {
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

  def leaseResource(channel: Channel, request: WorkerResource) : Option[ResourceLease] = request match {
    case c: ConcurrencyWorkerResource => leaseConcurrencyResource(channel, c)
    case t: ThrottledWorkerResource   => leaseThrottledResource(channel, t)
  }

  def leaseConcurrencyResource(channel: Channel, resource: ConcurrencyWorkerResource) : Option[ConcurrencyResourceLease] = {
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
    channelActor ! ChannelMessage(c => releaseResources(c, release), dropIfNoChannel = false)
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
      log.debug(s"Releasing lease ${ lease.inspect }")
      val props = AMQPMessageProperties.throttleTokenProperties(lease.resource)
      val body  = Array[Byte]()
      try {
        channel.txSelect()
        channel.basicAck(lease.deliveryTag, false)
        channel.basicPublish("", lease.resource.deferredQueueName, true, false, props, body)
        channel.txCommit()
        log.debug(s"Successfully released lease on ${ lease.inspect }")
      } catch {
        case e: Exception =>
          log.error(e, "Failed to publish resource token back to queue")
          channel.txRollback()
          throw e
      }
    } else {
      log.error(s"${lease.toString} found being released on unexpected channel! Found channel ${channel.getChannelNumber} instead of expected channel ${lease.channel.getChannelNumber}")
    }
  }

  /**
   * Sorts the resources into the ordering that they will be acquired in
   * @param resources The sequence of [[WorkerResource]] instances to be sorted
   * @return The ordering to use when attempting to acquire resources
   */
  def resourceAcquisitionOrder(resources: Seq[WorkerResource]) : Seq[WorkerResource] = {
    val concurrency = ArrayBuffer[ConcurrencyWorkerResource]()
    val throttling  = ArrayBuffer[ThrottledWorkerResource]()
    resources.collect {
      case c: ConcurrencyWorkerResource => concurrency.append(c)
      case t: ThrottledWorkerResource   => throttling.append(t)
    }
    (concurrency.sortBy(_.resourceName) ++ throttling.sortBy(_.resourceName)).toSeq
  }


  def leaseThrottledResource(channel: Channel, resource: ThrottledWorkerResource) : Option[ThrottledResourceLease] = {
    val response = channel.basicGet(resource.availableQueueName, false)
    if (response == null){
      log.debug(s"Resource ${ resource.inspect } is not currently available")
      None
    } else {
      log.info(s"Successfully acquired ${ resource.inspect }")
      Some(ThrottledResourceLease(resource, channel, response))
    }
  }
}

