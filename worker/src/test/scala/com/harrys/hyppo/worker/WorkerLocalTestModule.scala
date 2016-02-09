package com.harrys.hyppo.worker

import akka.actor.ActorSystem
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.harrys.hyppo.config.{HyppoWorkerModule, WorkerConfig}
import com.harrys.hyppo.worker.actor.data.LocalJarFileLoader
import com.harrys.hyppo.worker.data.{DataFileHandler, JarFileLoader, S3DataFileHandler}

/**
  * Created by jpetty on 2/9/16.
  */
class WorkerLocalTestModule(_config: WorkerConfig, _system: ActorSystem) extends HyppoWorkerModule(_config, _system) {

  override protected def bindJarFileHandler(): Unit = {
    bind(classOf[JarFileLoader]).to(classOf[LocalJarFileLoader])
  }

  override protected def bindDataFileHandler(): Unit = {
    val module = new FactoryModuleBuilder()
      .implement(classOf[DataFileHandler], classOf[S3DataFileHandler])
      .build(classOf[DataFileHandler.Factory])
    install(module)
  }


}
