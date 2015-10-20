package com.harrys.hyppo.worker.actor

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.harrys.hyppo.worker.TestConfig
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Created by jpetty on 9/23/15.
 */
abstract class WorkerActorTests extends TestKit(ActorSystem("TestActorSystem", TestConfig.basicTestConfig)) with WordSpecLike with BeforeAndAfterAll with Matchers with ImplicitSender {

  final override def afterAll() : Unit = {
    try {
      localTestCleanup()
    } finally {
      TestKit.shutdownActorSystem(system)
    }
  }

  def localTestCleanup() : Unit

}
