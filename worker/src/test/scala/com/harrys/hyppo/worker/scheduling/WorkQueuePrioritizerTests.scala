package com.harrys.hyppo.worker.scheduling

import java.time.temporal.ChronoUnit
import java.time._

import com.harrys.hyppo.source.api.model.IngestionSource
import com.harrys.hyppo.worker.{ProcessedDataStub, TestObjects, TestConfig}
import com.harrys.hyppo.worker.actor.amqp.{QueueNaming, SingleQueueDetails}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.harrys.hyppo.worker.api.proto.{ThrottledWorkResource, WorkResource}
import com.typesafe.config.ConfigFactory
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, Matchers, WordSpec}

import scala.collection.immutable.NumericRange

/**
  * Created by jpetty on 2/22/16.
  */
class WorkQueuePrioritizerTests extends PropSpec with Matchers with GeneratorDrivenPropertyChecks {

  property("prioritizes the queues based on the highest priority ordering first") {
    forAll(Gen.nonEmptyListOf(integrationWorkQueueDetailsGen), stableOrderingGen) {
      (items: List[SingleQueueDetails], ordering: List[PriorityOrdering]) =>
        val firstOrdering = ordering.head
        val prioritizer   = createPrioritizer(ordering)
        val topPriority   = prioritizer.prioritize(items).next
        firstOrdering.compare(topPriority, items.min(firstOrdering)) shouldBe 0
    }
  }

  property("retains the effective input size of the queues passed to it") {
    forAll(Gen.listOf(integrationWorkQueueDetailsGen), validOrderingGen) {
      (items: List[SingleQueueDetails], ordering: List[PriorityOrdering]) =>
        val prioritized = createPrioritizer(ordering).prioritize(items)
        prioritized.count(_ => true) shouldEqual items.length
    }
  }


  val config = TestConfig.workerWithRandomQueuePrefix()
  val naming = new QueueNaming(config)


  val stableOrderings = Seq[PriorityOrdering](
    ExpectedCompletionOrdering,
    AbsoluteSizeOrdering,
    IdleSinceMinuteOrdering
  )

  val unstableOrdering = Seq[PriorityOrdering](ShufflePriorityOrdering)

  val stableOrderingGen: Gen[List[PriorityOrdering]] = Gen.nonEmptyListOf(Gen.oneOf(stableOrderings))

  val validOrderingGen: Gen[List[PriorityOrdering]] = {
    Gen.nonEmptyListOf(Gen.oneOf(stableOrderings)).flatMap { stable =>
      Gen.listOf(Gen.oneOf(unstableOrdering)).map { unstable => stable ++ unstable }
    }
  }

  val workResourcesGen = CustomGens.workResourcesGen(naming)

  val integrationWorkQueueDetailsGen = CustomGens.integrationWorkQueueDetailsGen(naming, workResourcesGen)

  def createPrioritizer(ordering: List[PriorityOrdering]): WorkQueuePrioritizer = {
    val firstOrdering  = ordering.head
    val fallbackOrders = ordering.tail.toSeq
    WorkQueuePrioritizer.withNestedPriorities(firstOrdering, fallbackOrders:_*)
  }
}
