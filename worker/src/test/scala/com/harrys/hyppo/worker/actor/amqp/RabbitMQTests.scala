package com.harrys.hyppo.worker.actor.amqp

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.harrys.hyppo.worker.TestConfig
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Created by jpetty on 9/16/15.
 */
abstract class RabbitMQTests extends TestKit(ActorSystem("TestActorSystem", TestConfig.basicTestConfig)) with WordSpecLike with BeforeAndAfterAll with Matchers with ImplicitSender {

  final override def afterAll() : Unit = {
    TestKit.shutdownActorSystem(system)
  }

}
