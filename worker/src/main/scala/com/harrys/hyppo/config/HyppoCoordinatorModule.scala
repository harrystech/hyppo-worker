package com.harrys.hyppo.config

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.codahale.metrics.MetricRegistry
import com.google.inject.{AbstractModule, Provides}
import com.harrys.hyppo.HyppoCoordinator
import com.harrys.hyppo.coordinator.{WorkDispatcher, WorkResponseHandler}
import com.harrys.hyppo.worker.actor.amqp._
import com.sandinh.akuice.AkkaGuiceSupport

/**
  * Created by jpetty on 2/10/16.
  */
class HyppoCoordinatorModule  extends AbstractModule with AkkaGuiceSupport {

  override final def configure(): Unit = {
    requireBinding(classOf[WorkResponseHandler])
    requireBinding(classOf[MetricRegistry])
    requireBinding(classOf[ActorSystem])
    requireBinding(classOf[CoordinatorConfig])
    configureSpecializedBindings()
    bind(classOf[ResponseQueueConsumer])
    bind(classOf[EnqueueWorkQueueProxy])
    bind(classOf[WorkDispatcher]).to(classOf[HyppoCoordinator]).asEagerSingleton()
  }

  @Provides
  def hyppoConfig(config: CoordinatorConfig): HyppoConfig = config

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

  protected def configureSpecializedBindings(): Unit = ()
}
