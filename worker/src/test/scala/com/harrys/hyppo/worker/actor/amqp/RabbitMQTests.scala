package com.harrys.hyppo.worker.actor.amqp

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.harrys.hyppo.config.HyppoConfig
import com.harrys.hyppo.worker.TestConfig
import com.thenewmotion.akka.rabbitmq.ConnectionActor
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Created by jpetty on 9/16/15.
 */
abstract class RabbitMQTests[T <: HyppoConfig](final val config: T) extends TestKit(ActorSystem("TestActorSystem", TestConfig.basicTestConfig)) with WordSpecLike with BeforeAndAfterAll with Matchers with ImplicitSender {

  final override def afterAll() : Unit = {
    try {
      localTestCleanup()
    } finally {
      TestKit.shutdownActorSystem(system)
    }
  }

  def localTestCleanup() : Unit = {}

  final val connection = TestActorRef(ConnectionActor.props(config.rabbitMQConnectionFactory))
  final val naming  = new QueueNaming(config)
  final val helpers = new QueueHelpers(config, naming)

}
