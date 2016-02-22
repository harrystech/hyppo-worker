package com.harrys.hyppo.config

import javax.crypto.spec.SecretKeySpec

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.harrys.hyppo.worker.actor.amqp.{AMQPEncryption, QueueNaming, RabbitHttpClient}
import com.rabbitmq.client.ConnectionFactory
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * Created by jpetty on 8/27/15.
 */
abstract class HyppoConfig(config: Config) extends Serializable {

  //  Setup AWS Credentials either through explicit values or defaults from the environment / system properties / instance profile
  final def awsCredentialsProvider: AWSCredentialsProvider = {
    if (config.hasPath("hyppo.aws.access-key-id") && config.hasPath("hyppo.aws.secret-key")){
      new StaticCredentialsProvider(new BasicAWSCredentials(config.getString("hyppo.aws.access-key-id"), config.getString("hyppo.aws.secret-key")))
    } else {
      new DefaultAWSCredentialsProviderChain()
    }
  }

  //  Maximum wait time before work is given up on
  final val workTimeout: FiniteDuration = Duration(config.getDuration("hyppo.work-timeout").toMillis, MILLISECONDS)

  //  Location where date storage occurs
  final val dataBucketName: String = config.getString("hyppo.data-bucket-name")

  //  Prefix to insert at the front of all S3 content (code & data)
  final val storagePrefix: String = config.getString("hyppo.storage-prefix")

  //  Whether or not to print the configuration at application start
  final val printConfiguration: Boolean = config.getBoolean("hyppo.print-configuration")

  //  Timeout duration on operations involving rabbitmq
  final val rabbitMQTimeout: FiniteDuration = Duration(config.getDuration("hyppo.rabbitmq.timeout").toMillis, MILLISECONDS)

  //  Value to prefix all queue names with
  final val workQueuePrefix: String = config.getString("hyppo.work-queue.base-prefix")

  //  Amount of time to allow queues to linger in an inactive state
  final val workQueueTTL: FiniteDuration = Duration(config.getDuration("hyppo.work-queue.queue-ttl").toMillis, MILLISECONDS)

  //  Boolean flag that creates all queues as ephemeral, useful for testing in particular.
  final val allQueuesEphemeral: Boolean = config.getBoolean("hyppo.work-queue.all-ephemeral")

  //  Secret key to use when encrypting / decrypting RabbitMQ payloads. Must match on the workers and coordinator
  final val secretKey: SecretKeySpec = AMQPEncryption.initializeKeyFromSecret(config.getString("hyppo.secret-key"))

  //  The amount of time to allow for a graceful stop before forced termination
  final val shutdownTimeout: FiniteDuration = Duration(config.getDuration("hyppo.shutdown-timeout").toMillis, MILLISECONDS)

  /**
    * fractional amount of the [[shutdownTimeout]] that a single worker is allowed to wait before terminating
    */
  final val workerShutdownTimeout: FiniteDuration = Duration(shutdownTimeout.mul(0.8).toMillis, MILLISECONDS)

  //  Creates a rabbitMQ connection factory
  final def rabbitMQConnectionFactory: ConnectionFactory = {
    val factory = new ConnectionFactory()
    factory.setUri(config.getString("hyppo.rabbitmq.uri"))
    factory.setConnectionTimeout(rabbitMQTimeout.toMillis.toInt)
    if (config.hasPath("hyppo.rabbitmq.amqp-security-mode") &&
      !"none".equalsIgnoreCase(config.getString("hyppo.rabbitmq.amqp-security-mode"))){
      factory.useSslProtocol(config.getString("hyppo.rabbitmq.amqp-security-mode"))
    }
    factory
  }

  final val rabbitMQApiPort = config.getInt("hyppo.rabbitmq.rest-api-port")

  final val rabbitMQApiSSL  = config.getBoolean("hyppo.rabbitmq.rest-api-ssl")

  final def newRabbitMQApiClient(): RabbitHttpClient = {
    new RabbitHttpClient(rabbitMQConnectionFactory, rabbitMQApiPort, useSSL = rabbitMQApiSSL, new QueueNaming(this))
  }

  final def underlying: Config = config
}
