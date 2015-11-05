package com.harrys.hyppo.worker.actor.amqp

import java.time.Duration


/**
  * Created by jpetty on 11/4/15.
  */
object WorkerResources {

  sealed trait WorkerResource extends Serializable {
    def resourceName: String
    def inspect: String
  }

  @SerialVersionUID(1L)
  final case class ConcurrencyWorkerResource
  (
    override val resourceName: String,
    queueName: String,
    concurrency: Int
  ) extends WorkerResource {
    override def inspect: String = s"$productPrefix(name=$resourceName)"
  }

  @SerialVersionUID(1L)
  final case class ThrottledWorkerResource
  (
    override val resourceName: String,
    deferredQueueName: String,
    availableQueueName: String,
    throttleRate: Duration
  ) extends WorkerResource {

    if (throttleRate.isNegative){
      throw new IllegalArgumentException(s"The throttleRate must be greater than 0. Provided: ${ throttleRate.toString }")
    }

    override def inspect: String = s"$productPrefix(name=$resourceName rate=${ throttleRate.toString })"
  }
}
