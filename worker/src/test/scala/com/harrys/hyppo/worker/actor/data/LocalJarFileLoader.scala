package com.harrys.hyppo.worker.actor.data

import java.io.File
import java.nio.file.Files
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation
import com.harrys.hyppo.worker.data.{JarFileLoader, LoadedJarFile}
import org.apache.commons.io.{FileCleaningTracker, FileDeleteStrategy, FileUtils, FilenameUtils}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by jpetty on 2/9/16.
  */
class LocalJarFileLoader @Inject() (implicit @Assisted ec: ExecutionContext) extends JarFileLoader {

  private val tracker = new FileCleaningTracker()

  override def shutdown(): Unit = {
    tracker.exitWhenFinished()
  }

  override def loadJarFile(jar: RemoteStorageLocation): Future[LoadedJarFile] = {
    val loaded = LoadedJarFile(jar, createTrackedLocalCopy(jar))
    Future.successful(loaded)
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
