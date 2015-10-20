package com.harrys.hyppo.worker.exec

import java.io.File
import java.nio.file.Files

import com.harrys.hyppo.executor.cli.ExecutorMain
import com.harrys.hyppo.worker.proc.LaunchedExecutor

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
 * Created by jpetty on 7/29/15.
 */
final class ExecutorSetup() {
  private var environment = Map[String, String]()
  private val jvmArgs   = new ArrayBuffer[String]()
  private val classpath = new ArrayBuffer[File]()

  def addToEnv(values: (String, String)*) : Unit = {
    environment ++= values.toMap
  }

  def addToEnv(name: String, value: String) : Unit = {
    environment += (name -> value)
  }

  def addJvmArgs(args: String*) : Unit = this.addJvmArgs(args)

  def addJvmArgs(args: TraversableOnce[String]) : Unit = {
    jvmArgs.appendAll(args)
  }

  def prependJvmArgs(args: String*) : Unit = this.prependJvmArgs(args)

  def prependJvmArgs(args: TraversableOnce[String]) = jvmArgs.prependAll(args)

  def setJvmMinHeap(bytes: Long) : Unit = {
    val heapMin = "-Xms" + createHeapSizeArgument(bytes)
    jvmArgs.append(heapMin)
  }

  def setJvmMaxHeap(bytes: Long) : Unit = {
    val heapMax = "-Xmx" + createHeapSizeArgument(bytes)
    jvmArgs.append(heapMax)
  }

  def addToClasspath(entries: File*) : Unit = this.addToClasspath(entries)

  def addToClasspath(entries: TraversableOnce[File]) = {
    checkClasspathArgs(entries)
    classpath.appendAll(entries)
  }

  def prependToClasspath(entries: File*) : Unit = this.prependToClasspath(entries)

  def prependToClasspath(entries: TraversableOnce[File]) : Unit = {
    checkClasspathArgs(entries)
    classpath.prependAll(entries)
  }

  def launchWithArgs(commanderPort: Int, integrationClass: String) : LaunchedExecutor = {
    val execFiles = new ExecutorFiles(Files.createTempDirectory("context"))
    val builder   = new ProcessBuilder()
      .directory(execFiles.workingDirectory)
      .redirectError(execFiles.standardErrorFile)
      .redirectOutput(execFiles.standardOutFile)
      .command(this.toCommand(commanderPort, integrationClass):_*)
    //  Last chance to inject system values before launch
    environment += ("JAVA_HOME" -> javaHome.getAbsolutePath)
    //  Set the environment
    builder.environment().putAll(JavaConversions.mapAsJavaMap(environment))

    new LaunchedExecutor(builder.start(), execFiles)
  }

  def toCommand(commanderPort: Int, integrationClass: String) : Seq[String] = {
    val classPathArg = classpath.map(_.getAbsolutePath).mkString(File.pathSeparator)
    val jvmCommand = Seq[String](this.javaBin.getAbsolutePath, "-cp", classPathArg) ++ jvmArgs
    val properties = Seq[String](
      "-Dexecutor.integrationClass=" + integrationClass,
      "-Dexecutor.workerPort=" + commanderPort.toString
    )
    val appCommand = Seq[String](executorMainClass)
    jvmCommand ++ properties ++ appCommand
  }

  override def clone() : ExecutorSetup = {
    val result = new ExecutorSetup()
    result.jvmArgs.appendAll(this.jvmArgs)
    result.classpath.appendAll(this.classpath)
    result.environment ++= this.environment
    result
  }

  private def javaHome : File = new File(System.getProperty("java.home"))

  private def javaBin : File  = javaHome.toPath.resolve("bin").resolve("java").toFile

  private def executorMainClass: String = classOf[ExecutorMain].getCanonicalName

  private def checkClasspathArgs(entries: TraversableOnce[File]) : Unit = {
    val invalid = entries.filter(!_.exists())
    if (invalid.nonEmpty){
      throw new IllegalArgumentException(s"Provided files do not exist: ${invalid.map(_.getAbsolutePath).mkString(",")}")
    }
  }

  private def createHeapSizeArgument(bytes: Long) : String = {
    Math.ceil(bytes.toDouble / (1024 * 1024).toDouble).toInt.toString + "M"
  }
}
