package com.harrys.hyppo.worker

import java.util.{Date, UUID}

import com.harrys.hyppo.source.api.DataIntegration
import com.harrys.hyppo.source.api.model.{DataIngestionJob, DataIngestionTask, IngestionSource}
import com.harrys.hyppo.worker.api.code.{ExecutableIntegration, IntegrationCode, IntegrationDetails, IntegrationSchema}
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation
import com.typesafe.config.ConfigFactory

/**
 * Created by jpetty on 7/23/15.
 */
object TestObjects {

  def testIngestionSource() : IngestionSource = testIngestionSource(name = "Test Source")

  def testIngestionSource(name: String = "Test Source") : IngestionSource = {
    new IngestionSource(name, ConfigFactory.empty())
  }

  def testIngestionJob() : DataIngestionJob = testIngestionJob(testIngestionSource())

  def testIngestionJob(source: IngestionSource) : DataIngestionJob = {
    new DataIngestionJob(source, UUID.randomUUID(), ConfigFactory.empty(), new Date())
  }

  def testIngestionTask() : DataIngestionTask = testIngestionTask(testIngestionJob())

  def testIngestionTask(job: DataIngestionJob) : DataIngestionTask = {
    new DataIngestionTask(job, 1, ConfigFactory.empty())
  }


  def testLocalIntegrationCode(integration: DataIntegration[_]): IntegrationCode = {
    val jars  = TestConfig.testingClasspath().map(file => RemoteStorageLocation("LOCAL TEST", file.getAbsolutePath))
    IntegrationCode(integration.getClass.getCanonicalName, jars)
  }

  def testExecutableIntegration(source: IngestionSource, integration: DataIntegration[_]): ExecutableIntegration = {
    val schema    = IntegrationSchema(new ProcessedDataStub().avroType().recordSchema())
    val code      = testLocalIntegrationCode(integration)
    val semantics = integration.newProcessedDataPersister().semantics()
    ExecutableIntegration(source, schema, code, IntegrationDetails(isRawDataIntegration = false, persistingSemantics = semantics, 1))
  }


  def testProcessedDataIntegration(source: IngestionSource): ExecutableIntegration = {
    testExecutableIntegration(source, new ProcessedDataStub())
  }
}
