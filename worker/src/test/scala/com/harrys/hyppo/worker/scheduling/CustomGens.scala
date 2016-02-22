package com.harrys.hyppo.worker.scheduling

import java.time.temporal.ChronoUnit
import java.time._

import com.harrys.hyppo.source.api.model.IngestionSource
import com.harrys.hyppo.worker.actor.amqp.{QueueNaming, SingleQueueDetails}
import com.harrys.hyppo.worker.api.proto.WorkResource
import com.harrys.hyppo.worker.{TestObjects, ProcessedDataStub}
import com.harrys.hyppo.worker.api.code.ExecutableIntegration
import com.typesafe.config.ConfigFactory
import org.scalacheck.Gen

/**
  * Created by jpetty on 2/22/16.
  */
object CustomGens {

  val executableIntegrationGen: Gen[ExecutableIntegration] = {
    val integration = new ProcessedDataStub()
    val emptyConfig = ConfigFactory.empty()
    for {
      name <- Gen.identifier
    } yield TestObjects.testExecutableIntegration(new IngestionSource(name, emptyConfig), integration)
  }

  def recentLocalDateTimeGen(range: Duration): Gen[LocalDateTime] = {
    val max = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS)
    val min = max.minus(range)
    Gen.chooseNum(min.getEpochSecond, max.getEpochSecond).map(epoch => LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC))
  }

  def workResourcesGen(naming: QueueNaming): Gen[Seq[WorkResource]] = {
    val throttle = for {
      name <- Gen.identifier
      rate <- Gen.chooseNum(0L, 60L).map(Duration.ofSeconds)
    } yield naming.throttledResource(name, rate)
    val concurrency = for {
      name   <- Gen.identifier
      metric <- Gen.chooseNum(1, 5)
    } yield naming.concurrencyResource(name, metric)
    val singleResourceGen = Gen.frequency[WorkResource](
      (1, throttle),
      (3, concurrency)
    )
    Gen.oneOf(Gen.const(Seq[WorkResource]()), Gen.containerOfN[Seq, WorkResource](5, singleResourceGen))
  }

  def singleQueueDetailsGen(naming: QueueNaming, workResourcesGen: Gen[Seq[WorkResource]]): Gen[SingleQueueDetails] = {
    for {
      integration <- executableIntegrationGen
      resources   <- workResourcesGen
      size        <- Gen.chooseNum(0, 100, 0)
      rate        <- Gen.chooseNum(0.0, 5.0, 0.0)
      ready       <- Gen.chooseNum(0, size, 0, size)
      idle        <- recentLocalDateTimeGen(Duration.ofHours(1))
    } yield {
      val name    = naming.integrationWorkQueueName(integration, resources)
      val unacked = size - ready
      SingleQueueDetails(queueName = name, size = size, rate = rate, ready = ready, unacknowledged = unacked, idleSince = idle)
    }
  }
}
