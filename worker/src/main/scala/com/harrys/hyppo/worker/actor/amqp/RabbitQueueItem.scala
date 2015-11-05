package com.harrys.hyppo.worker.actor.amqp

import akka.actor.ActorRef
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, GetResponse}
import com.thenewmotion.akka.rabbitmq.{Channel, ChannelMessage}

/**
 * Created by jpetty on 9/21/15.
 */
final case class RabbitQueueItem(channel: ActorRef, response: GetResponse) {

  val properties: HyppoMessageProperties = AMQPMessageProperties.parseItemProperties(response.getProps)

  def envelope: Envelope = response.getEnvelope

  def deliveryTag: Long  = envelope.getDeliveryTag

  def replyToQueue: String = properties.replyToQueue

  def sendAck() : Unit = {
    channel ! ChannelMessage(c => sendAck(c), dropIfNoChannel = false)
  }

  private def sendAck(c: Channel) : Unit = {
    c.basicAck(deliveryTag, false)
  }

  def sendReject() : Unit = {
    channel ! ChannelMessage(c => sendReject(c), dropIfNoChannel = false)
  }

  private def sendReject(c: Channel) : Unit = {
    c.basicReject(deliveryTag, true)
  }

  def publishReply(body: Array[Byte]) : Unit = {
    publishReply(AMQPMessageProperties.replyProperties(properties), body)
  }

  def publishReply(props: BasicProperties, body: Array[Byte]) : Unit = {
    channel ! ChannelMessage(c => publishReply(c, props, body), dropIfNoChannel = false)
  }

  private def publishReply(channel: Channel, props: BasicProperties, body: Array[Byte]) : Unit = {
    channel.basicPublish("", properties.replyToQueue, true, props, body)
  }

  def printableDetails: String = {
    s"${this.productPrefix}(exchange=${envelope.getExchange} routingKey=${envelope.getRoutingKey} deliveryTag=${deliveryTag})"
  }
}
