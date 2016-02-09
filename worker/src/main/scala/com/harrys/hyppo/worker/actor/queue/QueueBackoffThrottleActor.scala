package com.harrys.hyppo.worker.actor.queue

import akka.actor.{Actor, ActorLogging}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.actor.amqp.{QueueDetails, QueueHelpers, QueueNaming, AMQPSerialization}
import com.harrys.hyppo.worker.api.proto.WorkResource

import scala.concurrent.duration.Deadline

/**
  * Created by jpetty on 2/9/16.
  */
final class QueueBackoffThrottleActor(config: WorkerConfig) extends Actor with ActorLogging {

  //  Backoff Management
  var backoffMap = Map[String, Deadline]()

  override def receive: Receive = {
    ???
  }
}


final case class QueueBackoff(name: String, attempts: Int, lastDetails: QueueDetails, resources: Seq[WorkResource])
