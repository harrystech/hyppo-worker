package com.harrys.hyppo.worker

import java.util.{Date, UUID}

import com.harrys.hyppo.source.api.PersistingSemantics
import com.harrys.hyppo.source.api.model.{DataIngestionJob, DataIngestionTask, IngestionSource}
import com.harrys.hyppo.worker.api.code.{ExecutableIntegration, IntegrationCode, IntegrationDetails, IntegrationSchema}
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

  def testProcessedDataIntegration(source: IngestionSource, semantics: PersistingSemantics = PersistingSemantics.Unsafe): ExecutableIntegration = {
    val schema = IntegrationSchema(new ProcessedDataStub().avroType().recordSchema())
    val code   = IntegrationCode(classOf[ProcessedDataStub].getCanonicalName, Seq())
    ExecutableIntegration(source, schema, code, IntegrationDetails(isRawDataIntegration = false, persistingSemantics = semantics, 1))
  }
}
