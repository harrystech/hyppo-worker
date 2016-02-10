package com.harrys.hyppo.worker.data

import java.io.IOException
import java.nio.file.Files
import javax.inject.Inject

import com.amazonaws.services.s3.AmazonS3Client
import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation
import org.apache.commons.io.{FileUtils, FileCleaningTracker, IOUtils, FilenameUtils}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by jpetty on 2/9/16.
  */

class S3JarFileLoader @Inject() (client: AmazonS3Client)(implicit @Assisted ec: ExecutionContext) extends JarFileLoader {
  private val tracker = new FileCleaningTracker()

  override def shutdown(): Unit = {
    tracker.exitWhenFinished()
  }

  override def loadJarFile(jar: RemoteStorageLocation): Future[LoadedJarFile] = Future {
    val s3Object = client.getObject(jar.bucket, jar.key)
    try {
      val tempFile = Files.createTempFile(FilenameUtils.removeExtension(FilenameUtils.getBaseName(jar.key)), "jar").toFile
      //  We're now tying the lifecycle of this file to the specific key that made the request
      try {
        FileUtils.copyInputStreamToFile(s3Object.getObjectContent, tempFile)
      } catch {
        case e: Exception =>
          FileUtils.deleteQuietly(tempFile)
          throw new IOException(s"Failed to download file from s3://${ jar.bucket }/${ jar.key }", e)
      }
      tracker.track(tempFile, jar)
      //  Since we return the result with the key attached, we can guarantee that the file lives as long as the cached value
      LoadedJarFile(jar, tempFile)
    } finally IOUtils.closeQuietly(s3Object)
  }
}
