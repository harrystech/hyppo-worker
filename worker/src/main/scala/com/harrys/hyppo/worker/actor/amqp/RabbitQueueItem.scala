package com.harrys.hyppo.worker.actor.amqp

import akka.actor.ActorRef
import com.github.sstone.amqp.Amqp.{Ack, Reject}
import com.rabbitmq.client.{Envelope, GetResponse}

/**
 * Created by jpetty on 9/21/15.
 */
final case class RabbitQueueItem(channel: ActorRef, response: GetResponse) {

  final val properties: HyppoMessageProperties = AMQPMessageProperties.parseItemProperties(response.getProps)

  def envelope: Envelope = response.getEnvelope

  def deliveryTag: Long = envelope.getDeliveryTag

  def replyToQueue: String = properties.replyToQueue

  def printableDetails: String = {
    s"${this.productPrefix}(exchange=${envelope.getExchange} routingKey=${envelope.getRoutingKey} deliveryTag=${deliveryTag})"
  }

  def createNack(requeue: Boolean = true) : Reject = Reject(deliveryTag, requeue = requeue)

  def createAck() : Ack = Ack(deliveryTag)
}
