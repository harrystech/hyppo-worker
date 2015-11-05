package com.harrys.hyppo.worker.actor.amqp.sync

import java.time.Duration

import akka.testkit.TestActorRef
import com.harrys.hyppo.util.TimeUtils
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.amqp.RabbitMQTests
import com.harrys.hyppo.worker.actor.sync.{ResourceLeaseAgent, ResourceNegotiation, ThrottledResourceLease}
import org.scalatest.concurrent.Eventually

import scala.util.Try

/**
  * Created by jpetty on 11/5/15.
  */
class LeaseAgentTests extends RabbitMQTests(TestConfig.workerWithRandomQueuePrefix()) with Eventually {

  val leaseActor = TestActorRef(new ResourceLeaseAgent(config, connection))
  val rabbitMQ   = config.rabbitMQConnectionFactory.newConnection()

  val throttleRate = Duration.ofMillis(10)
  val concurrentResource = naming.concurrencyResource("concurrency test", 2)
  val throttlingResource = naming.throttledResource("throttled test", throttleRate)

  /**
    * Wraps a given test block in an [[eventually]] clause that corresponds to the test's throttle rate. This is necessary
    * since sometimes actions in RabbitMQ aren't immediately available when under load (like running all tests)
    */
  private def amqpAsync[T](f: => T) : T = {
    val bounds = TimeUtils.scalaDuration(throttleRate) * 4
    val check  = bounds / 8
    eventually(timeout(bounds), interval(check))(f)
  }

  override def localTestCleanup() : Unit = {
    Try(helpers.destroyConcurrencyResource(concurrentResource))
    Try(helpers.destroyThrottledResource(throttlingResource))
    Try(rabbitMQ.close())
  }

  "The ResourceLeaseAgent" must {
    import ResourceNegotiation._

    "successfully create the concurrency queue" in {
      helpers.createConcurrencyResource(concurrentResource)
      amqpAsync {
        helpers.checkQueueSize(rabbitMQ, concurrentResource.queueName) shouldEqual concurrentResource.concurrency
        helpers.isQueueDefined(rabbitMQ, concurrentResource.queueName) shouldEqual true
      }
    }

    "make exactly two successful concurrency leases and successfully return them" in {
      leaseActor ! RequestForResources(Seq(concurrentResource))
      val one = expectMsgType[AcquiredResourceLeases]
      leaseActor ! RequestForResources(Seq(concurrentResource))
      val two = expectMsgType[AcquiredResourceLeases]
      leaseActor ! RequestForResources(Seq(concurrentResource))
      val three = expectMsgType[ResourceUnavailable]

      one.leases should have size 1
      two.leases should have size 1

      three.resourceName shouldEqual concurrentResource.resourceName

      leaseActor ! ReleaseResources(one.leases)
      amqpAsync(helpers.checkQueueSize(rabbitMQ, concurrentResource.queueName) shouldEqual 1)
      leaseActor ! ReleaseResources(two.leases)
      amqpAsync(helpers.checkQueueSize(rabbitMQ, concurrentResource.queueName) shouldEqual 2)
    }

    "successfully create the throttled queues" in {
      helpers.createThrottledResource(throttlingResource)
      amqpAsync {
        helpers.isQueueDefined(rabbitMQ, throttlingResource.deferredQueueName) shouldEqual true
        helpers.isQueueDefined(rabbitMQ, throttlingResource.availableQueueName) shouldEqual true
        val available = helpers.checkQueueSize(rabbitMQ, throttlingResource.availableQueueName)
        val deferred  = helpers.checkQueueSize(rabbitMQ, throttlingResource.deferredQueueName)
        available shouldEqual 1
        deferred shouldEqual 0
      }
    }

    "successfully lease the throttling item only, then release it successfully" in {
      leaseActor ! RequestForResources(Seq(throttlingResource))
      val result = expectMsgType[AcquiredResourceLeases]
      result.leases should have size 1

      result.leases.head shouldBe a[ThrottledResourceLease]
      val lease = result.leases.head.asInstanceOf[ThrottledResourceLease]
      lease.resource shouldEqual throttlingResource

      leaseActor ! RequestForResources(Seq(throttlingResource))
      expectMsgType[ResourceUnavailable].unavailable shouldEqual throttlingResource

      leaseActor ! ReleaseResources(Seq(lease))

      amqpAsync {
        val available = helpers.checkQueueSize(rabbitMQ, throttlingResource.availableQueueName)
        val deferred  = helpers.checkQueueSize(rabbitMQ, throttlingResource.deferredQueueName)
        if (available + deferred > 1){
          fail(s"Available plus deferred queue size should never be more than 1. Found: ${ available + deferred }")
        }
        available shouldEqual 1
        deferred shouldEqual 0
      }
    }
  }
}
