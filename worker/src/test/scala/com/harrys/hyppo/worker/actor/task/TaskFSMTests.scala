package com.harrys.hyppo.worker.actor.task

import java.io.IOException
import java.util.UUID

import akka.actor.{Kill, Terminated}
import akka.testkit.{TestFSMRef, TestProbe}
import com.harrys.hyppo.worker.actor.RabbitMQTests
import com.harrys.hyppo.worker.actor.task.TaskFSMStatus.{PerformingOperation, PreparingToStart, UploadingLogs}
import com.harrys.hyppo.worker.api.proto._
import com.harrys.hyppo.worker.{TestConfig, TestObjects}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

/**
 * Created by jpetty on 10/30/15.
 */
class TaskFSMTests extends RabbitMQTests("TaskFSMTests", TestConfig.workerWithRandomQueuePrefix()) with MockitoSugar with Eventually {

  val testSource  = TestObjects.testIngestionSource(name = "TaskFSM Test Source")
  val integration = TestObjects.testProcessedDataIntegration(source = testSource)
  val testJob     = TestObjects.testIngestionJob(testSource)

  "The TaskFSM" when {

    "downloading dependencies fails" must {
      val testSource  = TestObjects.testIngestionSource(name = "Special Source with Unique Name")
      val testJob     = TestObjects.testIngestionJob(testSource)
      val testTask    = TestObjects.testIngestionTask(testJob)
      val integration = TestObjects.testProcessedDataIntegration(source = testSource)
      val testRemote  = RemoteStorageLocation(config.dataBucketName, "")
      val testInput   = PersistProcessedDataRequest(integration, UUID.randomUUID(), Seq(), testTask, RemoteProcessedDataFile(testRemote, 0, Array[Byte](), 0))
      val channel     = connection.createChannel()
      val testItem    = enqueueThenDequeue(channel, testInput)
      val commander   = TestProbe()
      val taskFSM     = TestFSMRef(new TaskFSM(config, testItem, commander.ref))

      "hold the work exclusively" in {
        eventually {
          helpers.checkQueueSize(connection, testItem.headers.sourceQueue) shouldEqual 0
        }
      }

      "send the work item on initialization" in {
        commander.expectMsg(testItem.input)
        taskFSM.stateName should equal(PreparingToStart)
      }

      "restart with retry once the commander signals dependency failure" in {
        watch(taskFSM)
        commander.forward(taskFSM, TaskFSMEvent.TaskDependencyFailure(new IOException("Couldn't download stuff")))
        expectMsgType[Terminated]
        eventually {
          helpers.checkQueueSize(connection, testItem.headers.sourceQueue) shouldEqual 1
        }
      }
    }

    "handling idempotent work" must {
      val testInput     = CreateIngestionTasksRequest(integration, UUID.randomUUID(), Seq(), testJob)
      val channel       = connection.createChannel()
      val testExecution = enqueueThenDequeue(channel, testInput)
      val commander     = TestProbe()

      val taskFSM = TestFSMRef(new TaskFSM(config, testExecution, commander.ref))

      "send the work item once it initializes" in {
        commander.expectMsg(testExecution.input)
        taskFSM.stateName should equal(PreparingToStart)
      }

      "transition appropriately when the operation starts" in {
        taskFSM ! TaskFSMEvent.OperationStarting
        taskFSM.stateName should equal(PerformingOperation)
      }

      "transition and ACK when the results become available" in {
        val fakeTasks  = Seq(TestObjects.testIngestionTask(testJob))
        taskFSM ! TaskFSMEvent.OperationResponseAvailable(CreateIngestionTasksResponse(testInput, None, fakeTasks))
        taskFSM.stateName should equal(UploadingLogs)
        eventually {
          helpers.checkQueueSize(connection, testExecution.headers.replyToQueue) shouldEqual 1
        }
      }

      "transition and stop once the logs finish uploading" in {
        watch(taskFSM)
        taskFSM ! TaskFSMEvent.OperationLogUploaded
        expectMsgType[Terminated]
      }
    }

    "handling unsafe work" must {
      val testTask   = TestObjects.testIngestionTask(testJob)
      val testRemote = RemoteStorageLocation(config.dataBucketName, "")
      val testInput  = PersistProcessedDataRequest(integration, UUID.randomUUID(), Seq(), testTask, RemoteProcessedDataFile(testRemote, 0, Array[Byte](), 0))
      val channel    = connection.createChannel()
      val testItem   = enqueueThenDequeue(channel, testInput)
      val commander  = TestProbe()
      val taskFSM    = TestFSMRef(new TaskFSM(config, testItem, commander.ref))

      "send the work item on initialization" in {
        commander.expectMsg(testItem.input)
        taskFSM.stateName should equal(PreparingToStart)
      }

      "ack immediately once running starts and not requeue the work on failure" in {
        commander.send(taskFSM, TaskFSMEvent.OperationStarting)
        taskFSM.stateName should equal(PerformingOperation)
        watch(taskFSM)
        commander.testActor ! Kill
        expectTerminated(taskFSM)
      }
    }
  }
}
