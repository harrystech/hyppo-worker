package com.harrys.hyppo.worker.actor

import java.time.LocalDateTime
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization, RabbitQueueItem, WorkQueueItem}
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.rabbitmq.client.{Envelope, GetResponse}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

/**
 * Created by jpetty on 10/30/15.
 */
object TestAmqp extends MockitoSugar {

  private var deliveryTagCounter: Long = 0

  private def nextDeliveryTag: Long = {
    deliveryTagCounter += 1
    deliveryTagCounter
  }

  def fakeAmqpQueueItem(input: WorkerInput)(implicit system: ActorSystem) : GetResponse = {
    val serializer = new AMQPSerialization(system)
    val get        = mock[GetResponse]
    val envelope   = mock[Envelope]
      when(envelope.getDeliveryTag).thenReturn(nextDeliveryTag)
    when(get.getEnvelope).thenReturn(envelope)
    when(get.getProps).thenReturn(AMQPMessageProperties.enqueueProperties(UUID.randomUUID, "", LocalDateTime.now(), Duration(10, MINUTES)))
    when(get.getBody).thenReturn(serializer.serialize(input))

    get
  }


  def fakeRabbitQueueItem(channel: ActorRef, input: WorkerInput)(implicit system: ActorSystem) : RabbitQueueItem = {
    RabbitQueueItem(channel, fakeAmqpQueueItem(input))
  }

  def fakeWorkQueueItem(channel: ActorRef, input: WorkerInput)(implicit system: ActorSystem) : WorkQueueItem = {
    WorkQueueItem(fakeRabbitQueueItem(channel, input), input)
  }

}
