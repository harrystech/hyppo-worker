package com.harrys.hyppo.worker.actor.amqp

import akka.actor.ActorRef
import com.github.sstone.amqp.Amqp.QueueParameters

/**
 * Created by jpetty on 9/5/15.
 */
final case class QueueConsumer(queue: QueueParameters, consumerTag: String, consumer: ActorRef) {

  def queueName: String = queue.name

}
