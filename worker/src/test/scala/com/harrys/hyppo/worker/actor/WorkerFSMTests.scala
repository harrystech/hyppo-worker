package com.harrys.hyppo.worker.actor

import akka.testkit._
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.TestConfig


/**
 * Created by jpetty on 8/18/15.
 */
class WorkerFSMTests extends WorkerActorTests {

  override def localTestCleanup() : Unit = {}

  "The WorkerFSM" must {
    val workerFSM = TestFSMRef(new WorkerFSM(new WorkerConfig(TestConfig.basicTestConfig), self))

    "start in the idle state" in {
      workerFSM.stateName shouldEqual WorkerFSM.Idle
    }
    "poll for work immediately" in {
      expectMsg(RequestForAnyWork)
    }
    "activate the polling timer" in {
      workerFSM.isTimerActive(WorkerFSM.PollingTimerName) shouldBe true
    }
    "shut down regularly" in {
      workerFSM.stop()
    }
  }
}
