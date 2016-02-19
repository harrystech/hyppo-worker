package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.worker.actor.amqp.AMQPMessageProperties
import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, ThrottledWorkResource, WorkResource}
import com.rabbitmq.client._

import scala.util.Try

/**
 * Created by jpetty on 11/3/15.
 */
sealed trait ResourceLease {
  def resource: WorkResource
  def channel: Channel
  def token: GetResponse
  def inspect: String
  def release() : Unit
  def releaseUnconsumed(): Unit
  final def resourceName: String = resource.resourceName
  final def envelope: Envelope   = token.getEnvelope
  final def properties: BasicProperties = token.getProps
  final def deliveryTag: Long = envelope.getDeliveryTag
}

final case class ConcurrencyResourceLease
(
  override val resource: ConcurrencyWorkResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"

  override def release() : Unit = {
    channel.basicReject(deliveryTag, true)
  }

  override def releaseUnconsumed() : Unit = release()
}

final case class ThrottledResourceLease
(
  override val resource: ThrottledWorkResource,
  override val channel:  Channel,
  override val token:    GetResponse
) extends ResourceLease {

  override def inspect: String = s"${this.productPrefix}(name=$resourceName deliveryTag=$deliveryTag)"

  override def releaseUnconsumed() : Unit = {
    channel.basicReject(deliveryTag, true)
  }

  override def release() : Unit = {
    val props = AMQPMessageProperties.throttleTokenProperties(resource)
    channel.basicPublish("", resource.deferredQueueName, true, false, props, token.getBody)
    channel.basicAck(deliveryTag, false)
  }
}


final case class AcquiredResourceLeases(leases: Seq[ResourceLease]) {

  def releaseAllUnconsumed(): Unit = {
    leases.map { lease => Try(lease.releaseUnconsumed()) }.find(_.isFailure).foreach { failure => failure.get }
  }

  def releaseAll() : Unit = {
    leases.map { l => Try(l.release()) }.find(_.isFailure).foreach { failure => failure.get }
  }
}

final case class ResourceUnavailable(unavailable: WorkResource)
