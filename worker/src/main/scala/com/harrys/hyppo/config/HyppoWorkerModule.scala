package com.harrys.hyppo.config

import java.time.Duration

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.google.inject.{AbstractModule, Module, Provides}
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
    bind(classOf[QueueMetricsTracker]).to(workQueueManagerClass)
    bind(classOf[DelegationStrategy]).to(delegationStrategyClass)
    install(createJarFileHandlerFactory())
    install(createDataFileHandlerFactory())
    //  Setup injected actor bindings
    bindActorFactory[CommanderActor, CommanderActor.Factory]
    bindActorFactory[TaskFSM, TaskFSM.Factory]
    bindActorFactory[WorkerFSM, WorkerFSM.Factory]
    bindRabbitQueueStatusActorFactory()
  }

  protected def delegationStrategyClass: Class[_ <: DelegationStrategy] = classOf[DefaultDelegationStrategy]

  protected def workQueueManagerClass: Class[_ <: QueueMetricsTracker] = classOf[DefaultQueueMetricsTracker]

  protected def jarFileLoaderClass: Class[_ <: JarFileLoader] = classOf[S3JarFileLoader]

  protected def dataFileHandlerClass: Class[_ <: DataFileHandler] = classOf[S3DataFileHandler]

  protected def createJarFileHandlerFactory(): Module = {
    new FactoryModuleBuilder()
      .implement(classOf[JarFileLoader], jarFileLoaderClass)
      .build(classOf[JarFileLoader.Factory])
  }

  protected def createDataFileHandlerFactory(): Module = {
    new FactoryModuleBuilder()
      .implement(classOf[DataFileHandler], dataFileHandlerClass)
      .build(classOf[DataFileHandler.Factory])
  }


  protected def bindRabbitQueueStatusActorFactory(): Unit = {
    bindActorFactory[RabbitQueueStatusActor, RabbitQueueStatusActor.Factory]
  }

  @Provides
  def workQueuePrioritizer(): WorkQueuePrioritizer = {
    WorkQueuePrioritizer.withNestedPriorities(
      ExpectedCompletionOrdering,
      IdleSinceMinuteOrdering,
      AbsoluteSizeOrdering,
      ShufflePriorityOrdering
    )
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
