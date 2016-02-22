package com.harrys.hyppo.worker.actor.amqp

import javax.inject.Inject

import akka.actor.{Actor, ActorRef, Terminated}
import com.google.inject.assistedinject.Assisted

/**
  * Created by jpetty on 2/22/16.
  */
class NoOpQueueStatusActor @Inject()
(
  @Assisted("delegator") delegator: ActorRef
) extends Actor {

  context.watch(delegator)

  override def receive: Receive = {
    case Terminated(dead) if dead == delegator =>
      context.stop(self)
  }

}
