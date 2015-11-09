package com.harrys.hyppo.worker.actor

import akka.actor.{ActorRef, ActorSystem}
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization}
import com.harrys.hyppo.worker.actor.queue.{AcquiredResourceLeases, QueueItemHeaders, WorkQueueExecution, WorkQueueItem}
import com.harrys.hyppo.worker.api.proto.WorkerInput
import com.rabbitmq.client.{Channel, Envelope, GetResponse}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

/**
 * Created by jpetty on 10/30/15.
 */
object TestAmqp extends MockitoSugar {

  private var deliveryTagCounter: Long = 0
  private val serializer = new AMQPSerialization

  private def nextDeliveryTag: Long = {
    deliveryTagCounter += 1
    deliveryTagCounter
  }

  def fakeAmqpQueueItem(input: WorkerInput)(implicit system: ActorSystem) : GetResponse = {
    val get        = mock[GetResponse]
    val envelope   = mock[Envelope]
      when(envelope.getDeliveryTag).thenReturn(nextDeliveryTag)
      when(envelope.getExchange).thenReturn("")
    when(get.getEnvelope).thenReturn(envelope)
    when(get.getProps).thenReturn(AMQPMessageProperties.enqueueProperties(input.executionId, "", TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(Duration(10, MINUTES))))
    when(get.getBody).thenReturn(serializer.serialize(input))

    get
  }

  def fakeQueueItemHeaders(input: WorkerInput) : QueueItemHeaders = {
    val properties = AMQPMessageProperties.enqueueProperties(input.executionId, "", TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(Duration(10, MINUTES)))
    val envelope   = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(nextDeliveryTag)
    when(envelope.getExchange).thenReturn("")
    new QueueItemHeaders(envelope, properties)
  }

  def fakeWorkQueueItem(channel: ActorRef, input: WorkerInput)(implicit system: ActorSystem) : WorkQueueItem = {
    val headers = fakeQueueItemHeaders(input)
    WorkQueueItem(headers, input)
  }


  def fakeWorkQueueExecution(channel: Channel, input: WorkerInput)(implicit system: ActorSystem) : WorkQueueExecution = {
    val headers = fakeQueueItemHeaders(input)
    WorkQueueExecution(channel, headers, input, AcquiredResourceLeases(Seq()))
  }
}
