package com.harrys.hyppo.worker.actor.amqp

import java.io.IOException

import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.actor.amqp.Resources.{ConcurrencyResource, ThrottledResource}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.rabbitmq.client.{AMQP, Channel, Connection, ShutdownSignalException}

import scala.collection.JavaConversions
import scala.util.Try

/**
  * Created by jpetty on 11/4/15.
  */
final class QueueHelpers(config: HyppoConfig, naming: QueueNaming) {

  def this(config: HyppoConfig) = this(config, new QueueNaming(config))

  //  Used to define where to send "dead-letters" when defining a queue
  private val expiredExchangeHeader = "x-dead-letter-exchange"
  private val expiredQueueHeader    = "x-dead-letter-routing-key"
  //  Used to define the queue expiration behavior
  private val queueTLLHeader = "x-expires"

  def createResultsQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    channel.queueDeclare(naming.resultsQueueName, durable, exclusive, autoDelete, null)
  }

  def createExpiredQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    channel.queueDeclare(naming.expiredQueueName, durable, exclusive, autoDelete, null)
  }

  def createGeneralWorkQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    val arguments  = Map[String, String](
      expiredExchangeHeader -> "",
      expiredQueueHeader -> naming.expiredQueueName
    )
    channel.queueDeclare(naming.generalQueueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(arguments))
  }

  def createIntegrationQueue(channel: Channel, integration: ExecutableIntegration) : AMQP.Queue.DeclareOk = {
    val queueName  = naming.integrationQueueName(integration)
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    val arguments  = Map[String, AnyRef](
      expiredExchangeHeader -> "",
      expiredQueueHeader -> naming.expiredQueueName,
      queueTLLHeader     -> config.workQueueTTL.toMillis.underlying()
    )
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(arguments))
  }

  def createConcurrencyResource(resource: ConcurrencyResource) : ConcurrencyResource = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      createConcurrencyResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def createConcurrencyResource(connection: Connection, resource: ConcurrencyResource) : ConcurrencyResource = {
    passiveQueueDeclaration(connection, resource.queueName) match {
      case Some(declare) =>
        resource
      case None =>
        val channel = connection.createChannel()
        try {
          val durable    = true
          val exclusive  = false
          val autoDelete = false
          val declare    = channel.queueDeclare(resource.queueName, durable, exclusive, autoDelete, null)
          populateConcurrencyResource(channel, resource)
          resource
        } finally {
          Try(channel.close())
        }
    }
  }

  def createThrottledResource(resource: ThrottledResource) : ThrottledResource = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      createThrottledResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def createThrottledResource(connection: Connection, resource: ThrottledResource) : ThrottledResource = {
    val availableDeclare = passiveQueueDeclaration(connection, resource.availableQueueName)
    val deferredDeclare  = passiveQueueDeclaration(connection, resource.deferredQueueName)
    (availableDeclare, deferredDeclare) match {
      case (Some(_), Some(_)) =>
        resource
      case _ =>
        //  If both queues don't already exist, simply create the new ones from scratch directly
        val channel = connection.createChannel()
        try {
          createThrottledQueues(channel, resource)
          resource
        } finally {
          Try(channel.close())
        }
    }
  }

  def passiveQueueDeclaration(connection: Connection, queueName: String) : Option[AMQP.Queue.DeclareOk] = {
    val channel = connection.createChannel()
    try {
      Some(channel.queueDeclarePassive(queueName))
    } catch {
      case e: Exception if isQueueNotFoundException(e) => None
      case e: Exception => throw e
    } finally {
      Try(channel.close())
    }
  }

  def isQueueDefined(connection: Connection, queueName: String) : Boolean = {
    passiveQueueDeclaration(connection, queueName).isDefined
  }

  private def isQueueNotFoundException(root: Throwable) : Boolean = root match {
    case ioe: IOException =>
      Option(ioe.getCause).collect {
        case sse: ShutdownSignalException => sse.getReason
      }.collect {
        case close: AMQP.Channel.Close if close.getReplyCode == 404 => true
      }.getOrElse(false)
    case _ => false
  }

  private def createThrottledQueues(channel: Channel, resource: ThrottledResource) : (AMQP.Queue.DeclareOk, AMQP.Queue.DeclareOk) = {
    val durable      = true
    val exclusive    = false
    val autoDelete   = false
    val deferredArgs = Map[String, AnyRef](
      expiredExchangeHeader -> "",
      expiredQueueHeader -> resource.availableQueueName
    )
    val availableOk = channel.queueDeclare(resource.availableQueueName, durable, exclusive, autoDelete, null)
    val deferredOk  = channel.queueDeclare(resource.deferredQueueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(deferredArgs))
    (availableOk, deferredOk)
  }

  private def populateConcurrencyResource(channel: Channel, resource: ConcurrencyResource) : Unit = {
    (1 to resource.concurrency).inclusive.foreach { i =>
      channel.basicPublish("", resource.queueName, true, true, null, Array[Byte]())
    }
  }
}
