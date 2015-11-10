package com.harrys.hyppo.worker.actor.amqp

import java.io.IOException

import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.api.proto.{ConcurrencyWorkResource, IntegrationWorkerInput, ThrottledWorkResource}
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
  //  The default direct exchange for AMQP
  private val directExchange = ""
  //  Used to define the queue expiration behavior
  private val queueTLLHeader = "x-expires"
  //  Used to define maximum queue sizes
  private val queueSizeHeader = "x-max-size"


  def createResultsQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    val arguments  =
      if (config.allQueuesEphemeral){
        JavaConversions.mapAsJavaMap(Map(queueTLLHeader -> config.workQueueTTL.toMillis.underlying()))
      } else {
        null
      }
    channel.queueDeclare(naming.resultsQueueName, durable, exclusive, autoDelete, arguments)
  }

  def createExpiredQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    val arguments  =
      if (config.allQueuesEphemeral){
        JavaConversions.mapAsJavaMap(Map(queueTLLHeader -> config.workQueueTTL.toMillis.underlying()))
      } else {
        null
      }
    channel.queueDeclare(naming.expiredQueueName, durable, exclusive, autoDelete, arguments)
  }

  def createGeneralWorkQueue(channel: Channel) : AMQP.Queue.DeclareOk = {
    val durable    = true
    val exclusive  = false
    val autoDelete = false
    var arguments  = Map[String, AnyRef](
      expiredExchangeHeader -> directExchange,
      expiredQueueHeader -> naming.expiredQueueName
    )
    if (config.allQueuesEphemeral){
      arguments += (queueTLLHeader -> config.workQueueTTL.toMillis.underlying())
    }
    channel.queueDeclare(naming.generalQueueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(arguments))
  }

  def createIntegrationQueue(channel: Channel, input: IntegrationWorkerInput) : AMQP.Queue.DeclareOk = {
    val queueName  = naming.integrationWorkQueueName(input)
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

  def createConcurrencyResource(resource: ConcurrencyWorkResource) : ConcurrencyWorkResource = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      createConcurrencyResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def createConcurrencyResource(connection: Connection, resource: ConcurrencyWorkResource) : ConcurrencyWorkResource = {
    passiveQueueDeclaration(connection, resource.queueName) match {
      case Some(declare) =>
        resource
      case None =>
        val channel = connection.createChannel()
        try {
          val durable    = true
          val exclusive  = false
          val autoDelete = false
          val arguments  = Map[String, AnyRef](
            queueSizeHeader -> resource.concurrency.underlying(),
            queueTLLHeader  -> config.workQueueTTL.toMillis.underlying()
          )
          channel.queueDeclare(resource.queueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(arguments))
          populateConcurrencyResource(channel, resource)
          resource
        } finally {
          Try(channel.close())
        }
    }
  }

  def createThrottledResource(resource: ThrottledWorkResource) : ThrottledWorkResource = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      createThrottledResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def createThrottledResource(connection: Connection, resource: ThrottledWorkResource) : ThrottledWorkResource = {
    val availableDeclare = passiveQueueDeclaration(connection, resource.availableQueueName)
    val deferredDeclare  = passiveQueueDeclaration(connection, resource.deferredQueueName)
    (availableDeclare, deferredDeclare) match {
      case (Some(_), Some(_)) =>
        resource
      case _ =>
        //  If both queues don't already exist, simply create the new ones from scratch directly
        val channel = connection.createChannel()
        channel.confirmSelect()
        try {
          createThrottledQueues(channel, resource)
          val props = AMQPMessageProperties.throttleTokenProperties(resource)
          channel.basicPublish(directExchange, resource.deferredQueueName, true, false, props, Array[Byte]())
          channel.waitForConfirmsOrDie(config.rabbitMQTimeout.toMillis)
          resource
        } finally {
          Try(channel.close())
        }
    }
  }

  def destroyConcurrencyResource(resource: ConcurrencyWorkResource) : Unit = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      destroyConcurrencyResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def destroyConcurrencyResource(connection: Connection, resource: ConcurrencyWorkResource) : AMQP.Queue.DeleteOk = {
    val channel = connection.createChannel()
    try {
      channel.queueDelete(resource.queueName)
    } finally {
      Try(channel.close())
    }
  }

  def destroyThrottledResource(resource: ThrottledWorkResource) : (AMQP.Queue.DeleteOk, AMQP.Queue.DeleteOk) = {
    val connection = config.rabbitMQConnectionFactory.newConnection()
    try {
      destroyThrottledResource(connection, resource)
    } finally {
      Try(connection.close())
    }
  }

  def destroyThrottledResource(connection: Connection, resource: ThrottledWorkResource) : (AMQP.Queue.DeleteOk, AMQP.Queue.DeleteOk) = {
    val channel = connection.createChannel()
    try {
      val available = channel.queueDelete(resource.availableQueueName)
      val deferred  = channel.queueDelete(resource.deferredQueueName)
      (available, deferred)
    } finally {
      Try(channel.close())
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

  def checkQueueSize(connection: Connection, queueName: String) : Int = {
    passiveQueueDeclaration(connection, queueName).map(_.getMessageCount).getOrElse(0)
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

  private def createThrottledQueues(channel: Channel, resource: ThrottledWorkResource) : (AMQP.Queue.DeclareOk, AMQP.Queue.DeclareOk) = {
    val durable      = true
    val exclusive    = false
    val autoDelete   = false
    val deferredArgs = Map[String, AnyRef](
      expiredExchangeHeader -> directExchange,
      expiredQueueHeader    -> resource.availableQueueName,
      queueSizeHeader       -> 1.underlying(),
      queueTLLHeader        -> config.workQueueTTL.toMillis.underlying()
    )
    val availableArgs = Map[String, AnyRef](
      queueSizeHeader -> 1.underlying(),
      queueTLLHeader  -> config.workQueueTTL.toMillis.underlying()
    )
    val availableOk = channel.queueDeclare(resource.availableQueueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(availableArgs))
    val deferredOk  = channel.queueDeclare(resource.deferredQueueName, durable, exclusive, autoDelete, JavaConversions.mapAsJavaMap(deferredArgs))
    (availableOk, deferredOk)
  }

  private def populateConcurrencyResource(channel: Channel, resource: ConcurrencyWorkResource) : Unit = {
    channel.confirmSelect()
    (1 to resource.concurrency).inclusive.foreach { i =>
      channel.basicPublish(directExchange, resource.queueName, true, false, null, Array[Byte]())
    }
    channel.waitForConfirmsOrDie(config.rabbitMQTimeout.toMillis)
  }
}
