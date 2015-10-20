package com.harrys.hyppo.config

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * Created by jpetty on 8/27/15.
 */
final class CoordinatorConfig(config: Config) extends HyppoConfig(config) {

  //  Location to store jar files
  val codeBucketName = config.getString("hyppo.code-bucket-name")

  //  Time between logging metrics interval stats
  val queueMetricsInterval = Duration(config.getDuration("hyppo.work-queue.metrics-interval").toMillis, MILLISECONDS)

  //  Time between sweeping for expired items
  val queueSweepInterval = Duration(config.getDuration("hyppo.work-queue.cleanup-interval").toMillis, MILLISECONDS)

}
