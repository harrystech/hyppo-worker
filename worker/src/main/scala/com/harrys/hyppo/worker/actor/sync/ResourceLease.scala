package com.harrys.hyppo.worker.actor.sync

import com.harrys.hyppo.worker.actor.amqp.WorkerResources._
import com.rabbitmq.client._
import com.thenewmotion.akka.rabbitmq.Channel

/**
 * Created by jpetty on 11/3/15.
 */
sealed trait ResourceLease {
  def resource: WorkerResource
  def channel: Channel
  def token: GetResponse
  def inspect: String
  final def resourceName: String = resource.resourceName
  final def envelope: Envelope   = token.getEnvelope
  final def properties: BasicProperties = token.getProps
  final def deliveryTag: Long = envelope.getDeliveryTag
}

final case class ConcurrencyResourceLease
(
  override val resource: ConcurrencyWorkerResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"

}

final case class ThrottledResourceLease
(
  override val resource: ThrottledWorkerResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"
}