package com.harrys.hyppo.worker.actor.amqp


/**
  * Created by jpetty on 11/4/15.
  */
object Resources {

  sealed trait Resource extends Serializable {
    def resourceName: String
  }

  @SerialVersionUID(1L)
  final case class ConcurrencyResource(override val resourceName: String, queueName: String, concurrency: Int) extends Resource

  @SerialVersionUID(1L)
  final case class ThrottledResource(override val resourceName: String, deferredQueueName: String, availableQueueName: String) extends Resource
}
