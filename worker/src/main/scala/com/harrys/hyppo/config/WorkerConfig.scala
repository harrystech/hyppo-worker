package com.harrys.hyppo.config

import com.harrys.hyppo.worker.exec.ExecutorSetup
import com.typesafe.config.Config

import scala.collection.JavaConversions
import scala.concurrent.duration._

/**
 * Created by jpetty on 8/27/15.
 */
final class WorkerConfig(config: Config) extends HyppoConfig(config) {

  private val defaultSetup = {
    val setup = new ExecutorSetup()
    setup.setJvmMinHeap(config.getMemorySize("hyppo.executor.heap-min").toBytes)
    setup.setJvmMaxHeap(config.getMemorySize("hyppo.executor.heap-max").toBytes)
    setup.addJvmArgs(JavaConversions.asScalaBuffer(config.getStringList("hyppo.executor.jvm-opts")))
    setup
  }

  val workerCount: Int = config.getInt("hyppo.worker-count")

  if (workerCount <= 0){
    throw new IllegalArgumentException("Config value hyppo.worker-count must be > 0")
  }

  val workAffinityTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.work-affinity-timeout").toMillis, MILLISECONDS)

  val jarDownloadTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.jar-download-timeout").toMillis, MILLISECONDS)

  val taskPollingInterval: FiniteDuration = Duration(config.getDuration("hyppo.worker.task-polling-interval").toMillis, MILLISECONDS)

  val uploadDataTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.upload-data-timeout").toMillis, MILLISECONDS)

  val uploadTaskLog: Boolean = config.getBoolean("hyppo.worker.upload-task-log")

  val uploadLogTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.upload-log-timeout").toMillis, MILLISECONDS)

  def newExecutorSetup(): ExecutorSetup = defaultSetup.clone()

}
