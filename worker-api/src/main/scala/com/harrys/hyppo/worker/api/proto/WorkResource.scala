package com.harrys.hyppo.worker.api.proto

import java.time.Duration

/**
  * Created by jpetty on 11/6/15.
  */
sealed trait WorkResource extends Serializable {
  def resourceName: String
  def inspect: String
}

@SerialVersionUID(1L)
final case class ConcurrencyWorkResource
(
  override val resourceName: String,
  queueName: String,
  concurrency: Int
) extends WorkResource {
  if (concurrency <= 0){
    throw new IllegalArgumentException(s"$productPrefix instances must have a concurrency of 1 or more")
  }

  override def inspect: String = {
    s"${this.productPrefix}(name=$resourceName concurrency=$concurrency)"
  }
}

@SerialVersionUID(1L)
final case class ThrottledWorkResource
(
  override val resourceName: String,
  deferredQueueName: String,
  availableQueueName: String,
  throttleRate: Duration
) extends WorkResource {

  if (throttleRate.isNegative){
    throw new IllegalArgumentException(s"The throttleRate must be greater than 0. Provided: ${ throttleRate.toString }")
  }

  override def inspect: String = s"$productPrefix(name=$resourceName rate=${ throttleRate.toString })"
}
