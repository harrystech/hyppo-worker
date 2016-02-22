package com.harrys.hyppo.worker.scheduling

import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.actor.amqp.{SingleQueueDetails, QueueNaming}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.util.Random

/**
  * Created by jpetty on 2/22/16.
  */
class DefaultDelegationStrategyTests extends PropSpec with Matchers with GeneratorDrivenPropertyChecks {


  val config = TestConfig.workerWithRandomQueuePrefix()
  val naming = new QueueNaming(config)

  val workResourcesGen               = CustomGens.workResourcesGen(naming)
  val integrationWorkQueueDetailsGen = CustomGens.integrationWorkQueueDetailsGen(naming, workResourcesGen)
  val generalWorkQueueDetailsGen     = CustomGens.generalWorkQueueDetailsGen(naming)

  property("prefer general work if no affinity is provided") {
    forAll(
      CustomGens.validPrioritizerGen                      :| "prioritizer",
      Gen.nonEmptyListOf(integrationWorkQueueDetailsGen)  :| "integrationQueues",
      generalWorkQueueDetailsGen                          :| "generalQueue"
    ) {
      (prioritizer: WorkQueuePrioritizer, integrations: List[SingleQueueDetails], general: SingleQueueDetails) =>
        val strategy            = new DefaultDelegationStrategy(config, naming, prioritizer, Random)
        val integrationMetrics  = integrations.map(details => WorkQueueMetrics(details = details, resources = Seq()))
        val generalMetrics      = WorkQueueMetrics(details = general, resources = Seq())
        val combinedMetrics     = Seq(generalMetrics) ++ integrationMetrics

        val ordering            = strategy.priorityOrderWithoutAffinity(generalMetrics, integrationMetrics).toIndexedSeq

        whenever(combinedMetrics.exists(_.hasWork)) {
          if (general.hasWork){
            ordering.head shouldEqual general
          } else {
            ordering.head shouldNot equal(general)
          }
        }
    }
  }

  property("provide integration work when an affinity exists, falling back to the general queue if not") {
    val integrationGen = CustomGens.executableIntegrationGen
    val tuplizedGen    = integrationGen.flatMap { integration =>
      CustomGens.integrationWorkQueueDetailsGen(naming, integration, CustomGens.workResourcesGen(naming)).map { detail =>
        (integration, detail)
      }
    }

    forAll(
      CustomGens.validPrioritizerGen    :| "prioritizer",
      Gen.nonEmptyListOf(tuplizedGen)   :| "integrationQueues",
      generalWorkQueueDetailsGen        :| "generalQueue"
    ) {
      (prioritizer:   WorkQueuePrioritizer,
       integrations:  List[(ExecutableIntegration, SingleQueueDetails)],
       general:       SingleQueueDetails) =>
      {
        val strategy            = new DefaultDelegationStrategy(config, naming, prioritizer, Random)
        val integrationDetails  = integrations.map(_._2)
        val integrationMetrics  = integrationDetails.map(details => WorkQueueMetrics(details = details, resources = Seq()))
        val generalMetrics      = WorkQueueMetrics(details = general, resources = Seq())
        val combinedMetrics     = Seq(generalMetrics) ++ integrationMetrics
        val workerAffinity      = Random.shuffle(integrations).head._1

        whenever(combinedMetrics.exists(_.hasWork)) {
          val ordering  = strategy.priorityOrderWithPreference(workerAffinity, generalMetrics, integrationMetrics)
          val topChoice = ordering.next()

          if (combinedMetrics.exists(metric => metric.hasWork && naming.belongsToIntegration(workerAffinity)(metric.details.queueName))) {
            naming.belongsToIntegration(workerAffinity)(topChoice.queueName) shouldBe true
          } else if (general.hasWork) {
            topChoice shouldEqual general
          }
        }
      }
    }
  }





}
