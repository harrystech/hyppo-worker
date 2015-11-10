package com.harrys.hyppo.worker.actor.queue

import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.worker.api.proto.{PersistProcessedDataRequest, WorkerInput}
import com.rabbitmq.client.Channel

import scala.util.Try

/**
  * Created by jpetty on 11/6/15.
  */
final case class WorkQueueExecution
(
  channel:  Channel,
  headers:  QueueItemHeaders,
  input:    WorkerInput,
  leases:   AcquiredResourceLeases
) {

  val idempotent: Boolean = input match {
    case p: PersistProcessedDataRequest if p.integration.details.persistingSemantics == PersistingSemantics.Unsafe =>
      false
    case _ =>
      false
  }

  def tryWithChannel[T](action: (Channel) => T) : Try[T] = {
    Try(withChannel(action))
  }

  def withChannel[T](action: (Channel) => T) : T = this.synchronized {
    action(channel)
  }
}
