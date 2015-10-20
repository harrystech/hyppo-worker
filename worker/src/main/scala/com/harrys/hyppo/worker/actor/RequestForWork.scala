package com.harrys.hyppo.worker.actor

import com.harrys.hyppo.worker.api.code.ExecutableIntegration

/**
 * Created by jpetty on 9/5/15.
 */
sealed trait RequestForWork extends Serializable
@SerialVersionUID(1L)
case object RequestForAnyWork extends RequestForWork
@SerialVersionUID(1L)
final case class RequestForPreferredWork(prefer: ExecutableIntegration) extends RequestForWork
