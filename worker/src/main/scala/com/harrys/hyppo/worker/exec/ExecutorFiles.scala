package com.harrys.hyppo.worker.exec

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils

import scala.collection.JavaConversions

/**
 * Created by jpetty on 7/29/15.
 */
final class ExecutorFiles(val base: Path) {
  val workingDirectory  = Files.createDirectory(base.resolve("work")).toFile
  val logDirectory      = Files.createDirectory(base.resolve("log")).toFile
  //  By convention, all tasks are named using .out as the suffix for STDOUT stream files
  val initialStdOutFile = base.resolve("init.out").toFile
  //  The stderr stream is never reopened
  val standardErrorFile = base.resolve("stderr.err").toFile

  def totalSize : Long = {
    FileUtils.sizeOfDirectory(base.toFile)
  }

  def standardOutFiles: Seq[File] = {
    val files = JavaConversions.collectionAsScalaIterable(FileUtils.listFiles(logDirectory, Array("out"), true)).toSeq
    files.sortBy(_.getName).reverse
  }

  def lastTaskOutputFile: Option[File] = {
    standardOutFiles.lastOption
  }

  def lastStdoutFile: File = {
    lastTaskOutputFile.getOrElse(initialStdOutFile)
  }

  def workingDirectoryFiles : Seq[File] = {
    JavaConversions.collectionAsScalaIterable(FileUtils.listFiles(workingDirectory, null, true)).toSeq
  }

  def cleanupLogs() : Unit = {
    val logFiles = this.standardOutFiles
    if (logFiles.nonEmpty){
      logFiles.tail.foreach(FileUtils.deleteQuietly)
    }
  }

  def destroyAll() : Unit = {
    FileUtils.deleteDirectory(base.toFile)
  }
}
