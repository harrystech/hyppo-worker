package com.harrys.hyppo.config

import com.harrys.hyppo.worker.exec.{AvroFileCodec, ExecutorSetup, TaskLogStrategy}
import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory}

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

  val avroFileCodec: AvroFileCodec = new AvroFileCodec(config.getString("hyppo.worker.avro-file-codec"))

  val workerCount: Int = config.getInt("hyppo.worker-count")

  if (workerCount <= 0){
    throw new IllegalArgumentException("Config value hyppo.worker-count must be > 0")
  }

  val workAffinityTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.work-affinity-timeout").toMillis, MILLISECONDS)

  val jarDownloadTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.jar-download-timeout").toMillis, MILLISECONDS)

  val taskPollingInterval: FiniteDuration = Duration(config.getDuration("hyppo.worker.task-polling-interval").toMillis, MILLISECONDS)

  val uploadDataTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.upload-data-timeout").toMillis, MILLISECONDS)

  val taskLogStrategy: TaskLogStrategy = TaskLogStrategy.strategyForName(config.getString("hyppo.worker.task-log-strategy"))

  val uploadTaskLog: Boolean = if (taskLogStrategy == TaskLogStrategy.FileTaskLogStrategy) {
    config.getBoolean("hyppo.worker.upload-task-log")
  } else {
    false
  }

  val uploadLogTimeout: FiniteDuration = Duration(config.getDuration("hyppo.worker.upload-log-timeout").toMillis, MILLISECONDS)

  val resourceBackoffScaleFactor = config.getDouble("hyppo.worker.resource-throttle.backoff-scale-factor")
  if (resourceBackoffScaleFactor <= 0.0) {
    throw new IllegalArgumentException(s"hyppo.worker.resource-throttle.backoff-scale-factor must be > 0.0. Found: $resourceBackoffScaleFactor")
  }

  val resourceBackoffMinDelay = config.getDuration("hyppo.worker.resource-throttle.backoff-min-delay")
  if (resourceBackoffMinDelay.isZero || resourceBackoffMinDelay.isNegative) {
    throw new IllegalArgumentException(s"hyppo.worker.resource-throttle.backoff-min-delay must be > 0. Found: $resourceBackoffMinDelay")
  }

  val resourceBackoffMaxValue: FiniteDuration = Duration(config.getDuration("hyppo.worker.resource-throttle.backoff-max-wait").toMillis, MILLISECONDS)

  def newExecutorSetup(): ExecutorSetup = defaultSetup.clone()

  def withValue(path: String, value: ConfigValue): WorkerConfig = {
    new WorkerConfig(underlying.withValue(path, value))
  }

  def withValue(path: String, value: String): WorkerConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value))
  }

  def withValue(path: String, value: Int): WorkerConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Integer]))
  }

  def withValue(path: String, value: Boolean): WorkerConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Boolean]))
  }

  def withValue(path: String, value: Double): WorkerConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Double]))
  }
}
