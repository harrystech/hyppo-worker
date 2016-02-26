package com.harrys.hyppo.worker

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.worker.actor.amqp.{NoOpQueueStatusActor, RabbitQueueStatusActor}
import com.harrys.hyppo.worker.actor.data.{LocalDataFileHandler, LocalJarFileLoader}
import com.harrys.hyppo.worker.data.{DataFileHandler, JarFileLoader}

/**
  * Created by jpetty on 2/9/16.
  */
class WorkerLocalTestModule(system: ActorSystem, config: WorkerConfig) extends HyppoWorkerModule {

  override protected def jarFileLoaderClass: Class[_ <: JarFileLoader] = classOf[LocalJarFileLoader]

  override protected def dataFileHandlerClass: Class[_ <: DataFileHandler] = classOf[LocalDataFileHandler]

  override protected def bindRabbitQueueStatusActorFactory(): Unit = {
    bindActorFactory[NoOpQueueStatusActor, RabbitQueueStatusActor.Factory]
  }

  override protected def configureSpecializedBindings(): Unit = {
    bind(classOf[ActorSystem]).toInstance(system)
    bind(classOf[WorkerConfig]).toInstance(config)
    bind(classOf[MetricRegistry]).asEagerSingleton()
  }
}
