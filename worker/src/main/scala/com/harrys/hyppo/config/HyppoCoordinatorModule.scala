package com.harrys.hyppo.config

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.codahale.metrics.MetricRegistry
import com.google.inject.{AbstractModule, Provides}
import com.harrys.hyppo.HyppoCoordinator
import com.harrys.hyppo.coordinator.{WorkResponseHandler, WorkDispatcher}
import com.harrys.hyppo.worker.actor.amqp._

/**
  * Created by jpetty on 2/10/16.
  */
class HyppoCoordinatorModule(system: ActorSystem,config: CoordinatorConfig) extends AbstractModule {

  override def configure(): Unit = {
    requireBinding(classOf[WorkResponseHandler])
    bind(classOf[ActorSystem]).toInstance(system)
    bind(classOf[CoordinatorConfig]).toInstance(config)
    bind(classOf[HyppoConfig]).toInstance(config)
    bind(classOf[MetricRegistry]).asEagerSingleton()
    bind(classOf[ResponseQueueConsumer])
    bind(classOf[EnqueueWorkQueueProxy])
    bind(classOf[WorkDispatcher]).to(classOf[HyppoCoordinator]).asEagerSingleton()
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
  def createAmazonS3Client(config: CoordinatorConfig): AmazonS3Client = {
    new AmazonS3Client(config.awsCredentialsProvider)
  }



}
