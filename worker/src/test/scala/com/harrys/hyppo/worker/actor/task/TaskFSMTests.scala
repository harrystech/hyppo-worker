package com.harrys.hyppo.worker.actor.task

import akka.actor.Terminated
import akka.testkit.{TestFSMRef, TestProbe}
import com.github.sstone.amqp.Amqp
import com.github.sstone.amqp.Amqp.{Publish, Ack}
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.actor.{TestAmqp, WorkerFSMTests}
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.avro.AvroTest
import com.harrys.hyppo.worker.{ProcessedDataStub, TestConfig, TestObjects}
import org.scalatest.mock.MockitoSugar

/**
 * Created by jpetty on 10/30/15.
 */
class TaskFSMTests extends WorkerFSMTests with MockitoSugar {

  override def localTestCleanup() : Unit = {}

  val config = new WorkerConfig(TestConfig.basicTestConfig)

  val testSource  = TestObjects.testIngestionSource(name = "TaskFSM Test Source")
  val integration = TestObjects.testProcessedDataIntegration(source = testSource, semantics = PersistingSemantics.Unsafe)
  val testJob     = TestObjects.testIngestionJob(testSource)



  "The TaskFSM" when {
    "handling idempotent work" must {
      val testInput     = CreateIngestionTasksRequest(integration, testJob)
      val fakeChannel   = TestProbe()
      val fakeWorkItem  = TestAmqp.fakeWorkQueueItem(fakeChannel.ref, testInput)

      val taskFSM = TestFSMRef(new TaskFSM(config, fakeWorkItem, self))

      "send the work item once it initializes" in {
        expectMsg(fakeWorkItem.input)
        taskFSM.stateName should equal(PreparingToStart)
      }

      "transition appropriately when the operation starts" in {
        taskFSM ! TaskFSMEvent.OperationStarting
        taskFSM.stateName should equal(PerformingOperation)
      }

      "transition and ACK when the results become available" in {
        val fakeTasks = Seq(TestObjects.testIngestionTask(testJob))
        taskFSM ! TaskFSMEvent.OperationResultAvailable(CreateIngestionTasksResponse(testInput, RemoteLogFile(config.dataBucketName, ""), fakeTasks))
        taskFSM.stateName should equal(UploadingLogs)
        val ack = fakeChannel.expectMsgClass(classOf[Ack])
        fakeChannel.reply(Amqp.Ok(ack, None))
      }

      "transition and stop once the logs finish uploading" in {
        watch(taskFSM)
        taskFSM ! TaskFSMEvent.OperationLogUploaded
        expectMsgType[Terminated]
      }
    }

    "handling unsafe work" must {
      val testTask   = TestObjects.testIngestionTask(testJob)
      val fakeSingle = ProcessedTaskData(testTask, RemoteProcessedDataFile(config.dataBucketName, "", 1))
      val testInput  = PersistProcessedDataRequest(integration, testJob, Seq(fakeSingle))
      val channel    = TestProbe()
      val commander  = TestProbe()
      val testItem   = TestAmqp.fakeWorkQueueItem(channel.ref, testInput)

      val taskFSM   = TestFSMRef(new TaskFSM(config, testItem, commander.ref))

      "send the work item on initialization" in {
        commander.expectMsg(testItem.input)
        taskFSM.stateName should equal(PreparingToStart)
      }

      "ack immediately and transition appropriately when the operation starts" in {
        commander.send(taskFSM, TaskFSMEvent.OperationStarting)
        taskFSM.stateName should equal(PerformingOperation)
        val ack = channel.expectMsgClass(classOf[Ack])
        channel.reply(Amqp.Ok(ack, None))
      }

      "transition again once the results arrive" in {
        val fakeResult = PersistProcessedDataResponse(testInput, RemoteLogFile(config.dataBucketName, ""), fakeSingle)
        commander.send(taskFSM, TaskFSMEvent.OperationResultAvailable(fakeResult))
        val publish = channel.expectMsgClass(classOf[Publish])
        channel.reply(Amqp.Ok(publish, None))
        taskFSM.stateName should equal(UploadingLogs)
      }

      "stop fully after the log uploads finish" in {
        watch(taskFSM)
        commander.send(taskFSM, TaskFSMEvent.OperationLogUploaded)
        expectMsgType[Terminated]
      }
    }
  }
}
