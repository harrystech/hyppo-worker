package com.harrys.hyppo.worker.proto

import java.io.File
import java.net.{InetAddress, ServerSocket}

import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.source.api.DataIntegration
import com.harrys.hyppo.worker.TestConfig
import com.harrys.hyppo.worker.exec.AvroFileCodec
import com.harrys.hyppo.worker.proc.{LaunchedExecutor, SimpleCommander}
import org.scalatest._
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Seconds, Span}

import scala.io.Source

/**
 * Created by jpetty on 7/23/15.
 */
abstract class ExecutorCommandTest extends WordSpecLike with BeforeAndAfterAll with Matchers with TimeLimitedTests {

  override val timeLimit  = Span(20, Seconds)

  def integrationClass : Class[_ <: DataIntegration[_]]

  final def testClasspath   = System.getProperty("testing.classpath").split(":").map(new File(_)).filter(_.exists()).toSeq
  final def workerConfig    = new WorkerConfig(TestConfig.basicTestConfig)

  final val serverSocket    = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
  final val executorProcess = createExecutorProcess()
  final val commander       = new SimpleCommander(executorProcess, serverSocket)

  final override def afterAll() : Unit = {
    if (executorProcess.isAlive){
      executorProcess.killProcess()
    }
    executorProcess.deleteFiles()
    commander.forceShutdown()
  }

  override def withFixture(test: NoArgTest) : Outcome = {
    super.withFixture(test) match {
      case fail: Failed =>
        info(s"Executor Standard Error: \n${standardErrorContents()}\n")
        fail
      case other => other
    }
  }

  def standardOutContents() : String = Source.fromFile(executorProcess.files.lastStdoutFile).getLines().mkString("\n")

  def standardErrorContents() : String = Source.fromFile(executorProcess.files.standardErrorFile).getLines().mkString("\n")

  def createExecutorProcess() : LaunchedExecutor = {
    createExecutorProcess(integrationClass.getCanonicalName)
  }

  def createExecutorProcess(name: String) : LaunchedExecutor = {
    val setup = workerConfig.newExecutorSetup()
    setup.addToClasspath(testClasspath)
    setup.launchWithArgs(serverSocket.getLocalPort, name, workerConfig.taskLogStrategy, new AvroFileCodec("deflate"))
  }
}
