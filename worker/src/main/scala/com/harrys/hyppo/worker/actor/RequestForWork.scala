package com.harrys.hyppo.worker.actor

import akka.actor.ActorRef
import com.harrys.hyppo.worker.api.code.ExecutableIntegration

/**
 * Created by jpetty on 9/5/15.
 */
sealed trait RequestForWork extends Serializable
@SerialVersionUID(1L)
final case class RequestForAnyWork(channel: ActorRef) extends RequestForWork

@SerialVersionUID(1L)
final case class RequestForPreferredWork(channel: ActorRef, prefer: ExecutableIntegration) extends RequestForWork
