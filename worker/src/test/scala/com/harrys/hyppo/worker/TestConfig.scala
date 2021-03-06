package com.harrys.hyppo.worker

import java.io.File

import akka.actor.ActorSystem
import com.google.inject.{Guice, Injector}
import com.harrys.hyppo.HyppoWorker
import com.harrys.hyppo.config.{CoordinatorConfig, WorkerConfig}
import com.harrys.hyppo.coordinator.CoordinatorLocalTestModule
import com.harrys.hyppo.util.ConfigUtils
import com.typesafe.config.{Config, ConfigValueFactory}

import scala.util.Random

/**
 * Created by jpetty on 8/28/15.
 */
object TestConfig {

  def testingClasspath(): Seq[File] = {
    val path = System.getProperty("testing.classpath")
    val main = new File(classOf[HyppoWorker].getProtectionDomain.getCodeSource.getLocation.getFile).getAbsoluteFile
    val test = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile).getAbsoluteFile
    if (path == null) Seq(main, test) else path.split(":").map(new File(_)) ++ Seq(main, test)
  }

  def randomQueuePrefix(): String = {
    "hyppo-test-" + Random.alphanumeric.take(10).mkString
  }

  def basicTestConfig: Config = {
    ConfigUtils.resourceFileConfig("/hyppo-test.conf")
      .withFallback(HyppoWorker.referenceConfig())
      .resolve()
  }

  def basicTestWithQueuePrefix(prefix: String) : Config = {
    basicTestConfig.withValue("hyppo.work-queue.base-prefix", ConfigValueFactory.fromAnyRef(prefix)).resolve()
  }

  def basicTestWithRandomQueuePrefix: Config = {
    basicTestWithQueuePrefix(randomQueuePrefix())
  }

  def coordinatorWithRandomQueuePrefix(): CoordinatorConfig = {
    val random = randomQueuePrefix()
    val config = new CoordinatorConfig(basicTestWithQueuePrefix(random))
    assert(config.workQueuePrefix == random)
    config
  }

  def workerWithRandomQueuePrefix() : WorkerConfig = {
    val random = randomQueuePrefix()
    val config = new WorkerConfig(basicTestWithQueuePrefix(random))
    assert(config.workQueuePrefix == random)
    config
  }

  def localWorkerInjector(system: ActorSystem, config: WorkerConfig): Injector = {
    Guice.createInjector(new WorkerLocalTestModule(system, config))
  }

  def localCoordinatorInjector(system: ActorSystem, config: CoordinatorConfig): Injector = {
    Guice.createInjector(new CoordinatorLocalTestModule(system, config))
  }
}
