package com.harrys.hyppo.config

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.{Provides, AbstractModule}
import com.harrys.hyppo.worker.actor.amqp.{RabbitHttpClient, QueueHelpers, QueueNaming}
import com.sandinh.akuice.AkkaGuiceSupport

import scala.concurrent.{ExecutionContextExecutor, ExecutionContext}

/**
  * Created by jpetty on 2/10/16.
  */
class HyppoCoordinatorModule(system: ActorSystem, config: CoordinatorConfig) extends AbstractModule with AkkaGuiceSupport {

  override def configure(): Unit = {
    bind(classOf[ActorSystem]).toInstance(system)
    bind(classOf[CoordinatorConfig]).toInstance(config)
    bind(classOf[HyppoConfig]).toInstance(config)
  }


  @Provides
  def hyppoQueueNaming(config: CoordinatorConfig): QueueNaming = new QueueNaming(config)

  @Provides
  def hyppoQueueHelpers(config: CoordinatorConfig): QueueHelpers = new QueueHelpers(config, hyppoQueueNaming(config))

  @Provides
  def createRabbitMQClient(config: CoordinatorConfig): RabbitHttpClient = {
    new RabbitHttpClient(config.rabbitMQConnectionFactory, config.rabbitMQApiPort, config.rabbitMQApiSSL, hyppoQueueNaming(config))
  }

  @Provides
  def createAmazonS3Client(config: WorkerConfig): AmazonS3Client = {
    new AmazonS3Client(config.awsCredentialsProvider)
  }
}
