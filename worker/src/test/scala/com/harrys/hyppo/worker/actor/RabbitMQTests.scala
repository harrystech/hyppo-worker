package com.harrys.hyppo.worker.actor

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.actor.amqp.{AMQPMessageProperties, AMQPSerialization, QueueHelpers, QueueNaming}
import com.harrys.hyppo.worker.actor.queue.{QueueItemHeaders, ResourceLeasing, WorkQueueExecution}
import com.harrys.hyppo.worker.api.proto._
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.thenewmotion.akka.rabbitmq.ConnectionActor
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.Deadline
import scala.util.Try

/**
 * Created by jpetty on 9/16/15.
 */
abstract class RabbitMQTests[T <: HyppoConfig](systemName: String, final val config: T)
  extends TestKit(ActorSystem(systemName, config.underlying))
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ImplicitSender {

  override final def afterAll() : Unit = {
    try {
      localTestCleanup()
    } finally {
      Try(connection.close())
      TestKit.shutdownActorSystem(system)
    }
  }

  override final def beforeAll() : Unit = {
    withChannel { channel =>
      helpers.createExpiredQueue(channel).getQueue
      helpers.createGeneralWorkQueue(channel).getQueue
      helpers.createResultsQueue(channel).getQueue
    }
  }

  def localTestCleanup() : Unit = {}

  final val connection        = config.rabbitMQConnectionFactory.newConnection()
  final val connectionActor   = TestActorRef(ConnectionActor.props(new ConnectionFactory {
    override def newConnection() : Connection = connection
  }))

  final val naming     = new QueueNaming(config)
  final val helpers    = new QueueHelpers(config, naming)
  final val serializer = new AMQPSerialization(config.secretKey)
  final val leasing    = new ResourceLeasing

  protected def enqueueThenDequeue(channel: Channel, input: IntegrationWorkerInput)(implicit timeout: Timeout = Timeout(config.rabbitMQTimeout)) : WorkQueueExecution = {
    val queueName = enqueueWork(input)
    val deadline  = Deadline.now + timeout.duration
    var result: Option[WorkQueueExecution] = None
    while (result.isEmpty && deadline.hasTimeLeft()){
      result = dequeueExecution(channel, queueName)
    }
    result match {
      case Some(exec) => exec
      case None       =>
        throw new NoSuchElementException(s"Couldn't dequeue execution from ${ queueName } within ${ timeout.duration.toString }")
    }
  }

  protected def enqueueWork(input: GeneralWorkerInput) : String = {
    input.resources.foreach {
      case c: ConcurrencyWorkResource =>
        helpers.createConcurrencyResource(connection, c)
      case t: ThrottledWorkResource =>
        helpers.createThrottledResource(connection, t)
    }
    withChannel { c =>
      val body  = serializer.serialize(input)
      val props = AMQPMessageProperties.enqueueProperties(input.executionId, naming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
      c.basicPublish("", naming.generalQueueName, true, false, props, body)
      naming.generalQueueName
    }
  }

  protected def enqueueWork(input: IntegrationWorkerInput): String = {
    input.resources.foreach {
      case c: ConcurrencyWorkResource =>
        helpers.createConcurrencyResource(connection, c)
      case t: ThrottledWorkResource =>
        helpers.createThrottledResource(connection, t)
    }
    withChannel { c =>
      val name  = helpers.createIntegrationQueue(c, input).getQueue
      val body  = serializer.serialize(input)
      val props = AMQPMessageProperties.enqueueProperties(input.executionId, naming.resultsQueueName, TimeUtils.currentLocalDateTime(), TimeUtils.javaDuration(config.workTimeout))
      c.basicPublish("", name, true, false, props, body)
      name
    }
  }

  private def dequeueExecution(channel: Channel, name: String) : Option[WorkQueueExecution] = {
    val response    = channel.basicGet(name, false)
    if (response == null){
      None
    } else {
      val workerInput = serializer.deserialize[WorkerInput](response.getBody)
      val itemHeaders = new QueueItemHeaders(response.getEnvelope, response.getProps)
      val leased      = leasing.leaseResources(channel, workerInput.resources).left.get
      Some(WorkQueueExecution(channel, itemHeaders, workerInput, leased))
    }
  }

  protected def withChannel[A](action: (Channel) => A) : A = {
    val channel = connection.createChannel()
    try {
      action(channel)
    } finally {
      channel.close()
    }
  }
}
