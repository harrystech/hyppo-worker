package com.harrys.hyppo.worker.actor

import akka.testkit._
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.TestConfig


/**
 * Created by jpetty on 8/18/15.
 */
class WorkerFSMTests extends WorkerActorTests(new WorkerConfig(TestConfig.basicTestConfig)) {

  override def localTestCleanup() : Unit = {}

  "The WorkerFSM" must {
    val jarLoader = TestActorRef(new LocalJarLoadingActor())
    val workerFSM = TestFSMRef(new WorkerFSM(config, self, connectionActor, jarLoader))

    "start in the idle state" in {
      workerFSM.stateName shouldEqual WorkerFSM.Idle
    }
    "poll for work immediately" in {
      expectMsgType[RequestForAnyWork]
    }
    "activate the polling timer" in {
      workerFSM.isTimerActive(WorkerFSM.PollingTimerName) shouldBe true
    }
    "shut down regularly" in {
      workerFSM.stop()
    }
  }
}
