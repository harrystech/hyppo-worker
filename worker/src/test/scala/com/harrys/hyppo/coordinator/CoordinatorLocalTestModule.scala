package com.harrys.hyppo.coordinator

import akka.actor.ActorSystem
import com.google.inject.AbstractModule

/**
  * Created by jpetty on 2/26/16.
  */
class CoordinatorLocalTestModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[WorkResponseHandler]).to(classOf[NoOpWorkResponseHandler])
  }
}
