package com.harrys.hyppo.worker

import akka.actor.ActorSystem
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.worker.actor.amqp.{NoOpQueueStatusActor, RabbitQueueStatusActor}
import com.harrys.hyppo.worker.actor.data.{LocalDataFileHandler, LocalJarFileLoader}
import com.harrys.hyppo.worker.data.{DataFileHandler, JarFileLoader}

/**
  * Created by jpetty on 2/9/16.
  */
class WorkerLocalTestModule(_system: ActorSystem, _config: WorkerConfig) extends HyppoWorkerModule(_system, _config) {

  override protected def jarFileLoaderClass: Class[_ <: JarFileLoader] = classOf[LocalJarFileLoader]

  override protected def dataFileHandlerClass: Class[_ <: DataFileHandler] = classOf[LocalDataFileHandler]

  override protected def bindRabbitQueueStatusActorFactory(): Unit = {
    bindActorFactory[NoOpQueueStatusActor, RabbitQueueStatusActor.Factory]
  }
}
