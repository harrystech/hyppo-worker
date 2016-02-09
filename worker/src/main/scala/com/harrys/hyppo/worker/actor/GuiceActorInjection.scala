package com.harrys.hyppo.worker.actor

import akka.actor.{Props, Actor, IndirectActorProducer}
import com.google.inject.Injector

/**
  * Created by jpetty on 2/9/16.
  */
final class GuiceActorInjection[T <: Actor](injector: Injector, kind: Class[T]) extends IndirectActorProducer {
  override def produce(): Actor = injector.getInstance(kind)
  override def actorClass: Class[T] = kind
}

object GuiceActorInjection {

  def props[T <: Actor](injector: Injector, kind: Class[T]): Props = {
    Props(classOf[GuiceActorInjection[T]], injector, kind)
  }

}
