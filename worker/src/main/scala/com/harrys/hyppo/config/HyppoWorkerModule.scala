package com.harrys.hyppo.config

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.{Provides, Guice, Injector, AbstractModule}
import com.harrys.hyppo.worker.data.{S3JarFileLoader, S3DataFileHandler, DataFileHandler, JarFileLoader}

import scala.concurrent.{ExecutionContextExecutor, ExecutionContext}

/**
  * Created by jpetty on 2/9/16.
  */
class HyppoWorkerModule(val config: WorkerConfig, val system: ActorSystem) extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[WorkerConfig]).toInstance(config)
    bind(classOf[ActorSystem]).toInstance(system)
    bind(classOf[ExecutionContext]).toInstance(system.dispatcher)
    bind(classOf[ExecutionContextExecutor]).toInstance(system.dispatcher)
    bind(classOf[JarFileLoader]).to(jarFileHandler)
    bind(classOf[DataFileHandler]).to(dataFileHandler)
  }

  def jarFileHandler: Class[_ <: JarFileLoader] = classOf[S3JarFileLoader]

  def dataFileHandler: Class[_ <: DataFileHandler] = classOf[S3DataFileHandler]


  @Provides
  def createAmazonS3Client(config: WorkerConfig): AmazonS3Client = {
    new AmazonS3Client(config.awsCredentialsProvider)
  }
}
