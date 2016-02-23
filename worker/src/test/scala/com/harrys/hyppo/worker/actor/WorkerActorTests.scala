package com.harrys.hyppo.worker.actor

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.TestConfig
import com.rabbitmq.client.{Connection, ConnectionFactory}
import com.sandinh.akuice.ActorInject
import com.thenewmotion.akka.rabbitmq.ConnectionActor
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.util.Try

/**
 * Created by jpetty on 9/23/15.
 */
abstract class WorkerActorTests(val config: WorkerConfig)
  extends TestKit(ActorSystem("TestActorSystem", config.underlying))
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ImplicitSender
    with ActorInject {

  override val injector = TestConfig.localWorkerInjector(system, config)
  val workerFSMFactory  = injector.getInstance(classOf[WorkerFSM.Factory])
  val connection        = config.rabbitMQConnectionFactory.newConnection()
  val connectionActor   = system.actorOf(ConnectionActor.props(new ConnectionFactory {
    override def newConnection() : Connection = connection
  }), "rabbit-connection")



  final override def afterAll() : Unit = {
    try {
      localTestCleanup()
    } finally {
      Try(connection.close())
      TestKit.shutdownActorSystem(system)
    }
  }

  def localTestCleanup() : Unit


  def deleteQueues(queues: String*) : Unit = {
    val channel = connection.createChannel()
    try {
      queues.foreach { name =>
        channel.queueDelete(name)
      }
    } finally {
      channel.close()
    }
  }
}
