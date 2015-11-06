package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, ThrottledWorkResource, WorkResource}
import com.rabbitmq.client._

/**
 * Created by jpetty on 11/3/15.
 */
sealed trait ResourceLease {
  def resource: WorkResource
  def token: GetResponse
  def inspect: String
  final def resourceName: String = resource.resourceName
  final def envelope: Envelope   = token.getEnvelope
  final def properties: BasicProperties = token.getProps
  final def deliveryTag: Long = envelope.getDeliveryTag
}

final case class ConcurrencyResourceLease
(
  override val resource: ConcurrencyWorkResource,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"

}

final case class ThrottledResourceLease
(
  override val resource: ThrottledWorkResource,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"
}