package com.harrys.hyppo.coordinator

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.harrys.hyppo.config.{CoordinatorConfig, HyppoCoordinatorModule}

/**
  * Created by jpetty on 2/26/16.
  */
class CoordinatorLocalTestModule(system: ActorSystem, config: CoordinatorConfig) extends HyppoCoordinatorModule {

  override protected def configureSpecializedBindings(): Unit = {
    bind(classOf[WorkResponseHandler]).to(classOf[NoOpWorkResponseHandler])
    bind(classOf[MetricRegistry]).asEagerSingleton()
    bind(classOf[ActorSystem]).toInstance(system)
    bind(classOf[CoordinatorConfig]).toInstance(config)
  }
}
