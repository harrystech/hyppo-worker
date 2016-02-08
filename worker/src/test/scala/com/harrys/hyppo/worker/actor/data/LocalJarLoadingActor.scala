package com.harrys.hyppo.worker.actor.data

import java.io.File
import java.nio.file.Files

import akka.actor.Actor
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation
import com.harrys.hyppo.worker.data.LoadedJarFile
import com.harrys.hyppo.worker.data.JarLoadingActor.{JarsResult, LoadJars}
import org.apache.commons.io.{FileDeleteStrategy, FileCleaningTracker, FileUtils, FilenameUtils}

/**
  * Created by jpetty on 2/8/16.
  */
final class LocalJarLoadingActor extends Actor {

  private val tracker = new FileCleaningTracker()

  override def postStop(): Unit = {
    tracker.exitWhenFinished()
    System.gc()
  }

  override def receive: Receive = {
    case LoadJars(jars) =>
      val result = jars.map(copyLocalClasspathEntry)
      sender() ! JarsResult(result)
  }

  private def copyLocalClasspathEntry(remote: RemoteStorageLocation): LoadedJarFile = {
    val copied = createTrackedLocalCopy(remote)
    LoadedJarFile(remote, copied)
  }


  private def createTrackedLocalCopy(remote: RemoteStorageLocation): File = {
    val sourceFile = new File(remote.key)
    val copiedFile =
      if (sourceFile.isDirectory) {
        val tempDir = Files.createTempDirectory(FilenameUtils.getBaseName(sourceFile.getAbsolutePath)).toFile
        FileUtils.copyDirectory(sourceFile, tempDir)
        tempDir
      } else if (sourceFile.isFile) {
        val tempFile = Files.createTempFile(FilenameUtils.removeExtension(sourceFile.getName), FilenameUtils.getExtension(sourceFile.getName)).toFile
        FileUtils.copyFile(sourceFile, tempFile)
        tempFile
      } else {
        throw new IllegalArgumentException(s"Classpath location ${ sourceFile.getAbsolutePath } does not exist!")
      }
    tracker.track(copiedFile, remote, FileDeleteStrategy.FORCE)
    copiedFile
  }
}
