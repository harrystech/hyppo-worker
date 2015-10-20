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
  val standardOutFile   = base.resolve("stdout.out").toFile
  val standardErrorFile = base.resolve("stderr.out").toFile

  def totalSize : Long = {
    FileUtils.sizeOfDirectory(base.toFile)
  }

  def workingDirectoryFiles : Seq[File] = {
    JavaConversions.collectionAsScalaIterable(FileUtils.listFiles(workingDirectory, null, true)).toSeq
  }

  def destroyAll() : Unit = {
    FileUtils.deleteDirectory(base.toFile)
  }
}
