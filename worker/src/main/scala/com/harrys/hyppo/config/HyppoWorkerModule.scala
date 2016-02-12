package com.harrys.hyppo.config

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.google.inject.{Provides, Guice, Injector, AbstractModule}
import com.harrys.hyppo.worker.actor.amqp.{RabbitQueueStatusActor, QueueHelpers, QueueNaming, RabbitHttpClient}
import com.harrys.hyppo.worker.actor.queue._
import com.harrys.hyppo.worker.actor.{WorkerFSM, CommanderActor}
import com.harrys.hyppo.worker.actor.task.TaskFSM
import com.harrys.hyppo.worker.data.{S3JarFileLoader, S3DataFileHandler, DataFileHandler, JarFileLoader}
import com.harrys.hyppo.worker.scheduling._
import com.sandinh.akuice.AkkaGuiceSupport

import scala.concurrent.{ExecutionContextExecutor, ExecutionContext}

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
    bindDelgationStrategy()
    bindDelgationStrategy()
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

  protected def bindDelgationStrategy(): Unit = {
    bind(classOf[QueueStatusTracker]).to(classOf[DefaultQueueStatusTracker])
    val priorities = WorkQueuePrioritizer
      .withNestedPriorities(
        ExpectedCompletionOrdering,
        IdleSinceMinuteOrdering,
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
  def createRabbitMQClient(config: WorkerConfig): RabbitHttpClient = {
    new RabbitHttpClient(config.rabbitMQConnectionFactory, config.rabbitMQApiPort, config.rabbitMQApiSSL, hyppoQueueNaming(config))
  }

  @Provides
  def createAmazonS3Client(config: WorkerConfig): AmazonS3Client = {
    new AmazonS3Client(config.awsCredentialsProvider)
  }
}
