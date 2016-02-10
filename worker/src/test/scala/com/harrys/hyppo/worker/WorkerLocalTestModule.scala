package com.harrys.hyppo.worker

import akka.actor.ActorSystem
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.worker.actor.data.{LocalDataFileHandler, LocalJarFileLoader}
import com.harrys.hyppo.worker.data.{S3JarFileLoader, DataFileHandler, JarFileLoader}

/**
  * Created by jpetty on 2/9/16.
  */
class WorkerLocalTestModule(_system: ActorSystem, _config: WorkerConfig) extends HyppoWorkerModule(_system, _config) {

  override protected def bindJarFileHandler(): Unit = {
    val module = new FactoryModuleBuilder()
      .implement(classOf[JarFileLoader], classOf[LocalJarFileLoader])
      .build(classOf[JarFileLoader.Factory])
    install(module)
  }

  override protected def bindDataFileHandler(): Unit = {
    val module = new FactoryModuleBuilder()
      .implement(classOf[DataFileHandler], classOf[LocalDataFileHandler])
      .build(classOf[DataFileHandler.Factory])
    install(module)
  }


}
