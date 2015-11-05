package com.harrys.hyppo.worker.actor.sync

import com.harrys.hyppo.worker.actor.amqp.Resources._
import com.rabbitmq.client._
import com.thenewmotion.akka.rabbitmq.Channel

/**
 * Created by jpetty on 11/3/15.
 */
sealed trait ResourceLease {
  def resource: Resource
  def channel: Channel
  def token: GetResponse
  final def resourceName: String = resource.resourceName
  final def envelope: Envelope   = token.getEnvelope
  final def properties: BasicProperties = token.getProps
  final def deliveryTag: Long = envelope.getDeliveryTag
}

final case class ConcurrencyResourceLease
(
  override val resource: ConcurrencyResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease

final case class ThrottledResourceLease
(
  override val resource: ThrottledResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease {

  channel.addShutdownListener(new ShutdownListener {
    override def shutdownCompleted(cause: ShutdownSignalException): Unit = ???
  })
  throw new NotImplementedError(this.getClass.getName + " has not been implemented yet")
}