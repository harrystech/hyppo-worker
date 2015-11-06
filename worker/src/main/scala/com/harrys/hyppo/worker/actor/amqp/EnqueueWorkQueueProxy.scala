package com.harrys.hyppo.worker.actor.amqp

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.harrys.hyppo.config.CoordinatorConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.api.proto.{GeneralWorkerInput, IntegrationWorkerInput}
import com.thenewmotion.akka.rabbitmq._

/**
 * Created by jpetty on 9/16/15.
 */
final class EnqueueWorkQueueProxy(config: CoordinatorConfig, connection: ActorRef) extends Actor with ActorLogging {

  val serializer   = new AMQPSerialization(context)
  val queueNaming  = new QueueNaming(config)
  val queueHelpers = new QueueHelpers(config, queueNaming)

  val channelActor = connection.createChannel(ChannelActor.props((channel: Channel, self: ActorRef) => {
    queueHelpers.createExpiredQueue(channel)
    queueHelpers.createResultsQueue(channel)
    queueHelpers.createGeneralWorkQueue(channel)
  }))

  override def receive: Receive = {
    case work: GeneralWorkerInput     =>
      channelActor ! ChannelMessage((c: Channel) => publishWithChannel(c, work), dropIfNoChannel = false)
    case work: IntegrationWorkerInput =>
      channelActor ! ChannelMessage((c: Channel) => publishWithChannel(c, work), dropIfNoChannel = false)
  }

  def publishWithChannel(channel: Channel, work: GeneralWorkerInput) : Unit = {
    val body  = serializer.serialize(work)
    val props = AMQPMessageProperties.enqueueProperties(work.executionId, queueNaming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
    channel.basicPublish("", queueNaming.generalQueueName, true, false, props, body)
  }

  def publishWithChannel(channel: Channel, work: IntegrationWorkerInput) : Unit = {
    val queue = queueHelpers.createIntegrationQueue(channel, work).getQueue
    val body  = serializer.serialize(work)
    val props = AMQPMessageProperties.enqueueProperties(work.executionId, queueNaming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
    channel.basicPublish("", queue, true, false, props, body)
  }
}
