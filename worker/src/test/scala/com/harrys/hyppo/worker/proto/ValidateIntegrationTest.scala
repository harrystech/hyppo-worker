package com.harrys.hyppo.worker.proto

import com.harrys.hyppo.executor.proto.com.ValidateIntegrationCommand
import com.harrys.hyppo.executor.proto.res.ValidateIntegrationResult
import com.harrys.hyppo.worker.{ProcessedDataStub, TestObjects, RawDataStub}
import org.apache.avro.Schema

import scala.concurrent.duration._

/**
 * Created by jpetty on 7/23/15.
 */
class ValidateIntegrationTest extends ExecutorCommandTest {

  override def integrationClass = classOf[RawDataStub]

  "The validate integration test" must {
    val validateResult = commander.executeCommand(new ValidateIntegrationCommand(TestObjects.testIngestionSource()))

    "produce a result of the correct type" in {
      validateResult shouldBe a [ValidateIntegrationResult]
    }

    val result = validateResult.asInstanceOf[ValidateIntegrationResult]
    "wait for the result object" in {
      result.getSchema shouldBe a [Schema]
      result.getSchema shouldEqual new ProcessedDataStub().avroType().recordSchema()
    }

    "then exit cleanly" in {
      commander.sendExitCommandAndWait(Duration(100, MILLISECONDS)) shouldEqual 0
    }
  }
}
