package com.harrys.hyppo.worker.data

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jpetty on 8/5/15.
 */
final class TempFilePool(path: Path) {

  private val leased = new ArrayBuffer[File]()

  def newFile(prefix: String, suffix: String) : File = {
    val tempFile = Files.createTempFile(path, prefix, suffix).toFile
    leased.synchronized {
      leased.append(tempFile)
    }
    tempFile
  }

  def cleanAll() : Unit = {
    leased.synchronized {
      leased.foreach(FileUtils.deleteQuietly)
      leased.clear()
    }
  }
}
