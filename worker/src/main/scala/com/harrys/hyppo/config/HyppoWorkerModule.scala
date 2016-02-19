package com.harrys.hyppo.config

import java.time.Duration

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.google.inject.{AbstractModule, Provides}
import com.harrys.hyppo.worker.actor.amqp.{QueueHelpers, QueueNaming, RabbitHttpClient, RabbitQueueStatusActor}
import com.harrys.hyppo.worker.actor.queue._
import com.harrys.hyppo.worker.actor.task.TaskFSM
import com.harrys.hyppo.worker.actor.{CommanderActor, WorkerFSM}
import com.harrys.hyppo.worker.data.{DataFileHandler, JarFileLoader, S3DataFileHandler, S3JarFileLoader}
import com.harrys.hyppo.worker.scheduling._
import com.sandinh.akuice.AkkaGuiceSupport

/**
  * Created by jpetty on 2/9/16.
  */
class HyppoWorkerModule( val system: ActorSystem, val config: WorkerConfig) extends AbstractModule with AkkaGuiceSupport {

  override final def configure(): Unit = {
    bind(classOf[WorkerConfig]).toInstance(config)
    bind(classOf[ActorSystem]).toInstance(system)
    //  Opportunity for overrides
    bindJarFileHandler()
    bindDataFileHandler()
    bindDelegationStrategy()
    //  Setup injected actor bindings
    bindActorFactory[CommanderActor, CommanderActor.Factory]
    bindActorFactory[TaskFSM, TaskFSM.Factory]
    bindActorFactory[WorkerFSM, WorkerFSM.Factory]
    bindActorFactory[RabbitQueueStatusActor, RabbitQueueStatusActor.Factory]
  }

  protected def bindJarFileHandler(): Unit = {
    val module = new FactoryModuleBuilder()
      .implement(classOf[JarFileLoader], classOf[S3JarFileLoader])
      .build(classOf[JarFileLoader.Factory])
    install(module)
  }

  protected def bindDataFileHandler(): Unit = {
    val module = new FactoryModuleBuilder()
      .implement(classOf[DataFileHandler], classOf[S3DataFileHandler])
      .build(classOf[DataFileHandler.Factory])
    install(module)
  }

  protected def bindDelegationStrategy(): Unit = {
    bind(classOf[QueueStatusTracker]).to(classOf[DefaultQueueStatusTracker])
    val priorities = WorkQueuePrioritizer
      .withNestedPriorities(
        ExpectedCompletionOrdering,
        IdleSinceMinuteOrdering,
        AbsoluteSizeOrdering,
        ShufflePriorityOrdering
      )
    bind(classOf[WorkQueuePrioritizer]).toInstance(priorities)
    val module = new FactoryModuleBuilder()
      .implement(classOf[DelegationStrategy], classOf[DefaultDelegationStrategy])
      .build(classOf[DelegationStrategy.Factory])
    install(module)
  }

  @Provides
  def hyppoQueueNaming(config: WorkerConfig): QueueNaming = new QueueNaming(config)

  @Provides
  def hyppoQueueHelpers(config: WorkerConfig): QueueHelpers = new QueueHelpers(config, hyppoQueueNaming(config))

  @Provides
  def recentWorkQueueContention(config: WorkerConfig): RecentResourceContention = {
    val retention = Duration.ofMillis(config.resourceBackoffMaxValue.toMillis)
    new RecentResourceContention(retention)
  }

  @Provides
  def createRabbitMQClient(config: WorkerConfig): RabbitHttpClient = {
    new RabbitHttpClient(config.rabbitMQConnectionFactory, config.rabbitMQApiPort, config.rabbitMQApiSSL, hyppoQueueNaming(config))
  }

  @Provides
  def createAmazonS3Client(config: WorkerConfig): AmazonS3Client = {
    new AmazonS3Client(config.awsCredentialsProvider)
  }
}
