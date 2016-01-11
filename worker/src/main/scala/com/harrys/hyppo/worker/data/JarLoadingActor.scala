package com.harrys.hyppo.worker.data

import java.io.File
import java.nio.file.Files

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3Object
import com.harrys.hyppo.config.WorkerConfig
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation
import org.apache.commons.io.{IOUtils, FileCleaningTracker, FileUtils, FilenameUtils}

import scala.concurrent.Future

final class JarLoadingActor(config: WorkerConfig) extends Actor with ActorLogging {
  import JarLoadingActor._
  //  Load the dispatcher as the default execution context
  import context.dispatcher

  private val client   = new AmazonS3Client(config.awsCredentialsProvider)
  private val tracker  = new FileCleaningTracker()

  override def postStop(): Unit = {
    tracker.exitWhenFinished()
    super.postStop()
  }

  override def receive: Receive = {
    case LoadJars(jarFiles) =>
      loadJarFiles(jarFiles, sender())
  }

  def loadJarFiles(jarFiles: Seq[RemoteStorageLocation], recipient: ActorRef) : Unit = {
    val downloads = jarFiles.map(jar => loadedJarFuture(jar))
    val combined  = Future.sequence(downloads).map(jars => JarsResult(jars))
    combined.pipeTo(recipient)
  }


  private def loadedJarFuture(jar: RemoteStorageLocation) : Future[LoadedJarFile] = Future {
    log.debug("Loading Jar File From S3: {}", jar)
    val s3Object = client.getObject(jar.bucket, jar.key)
    try {
      val tempFile = downloadToFile(s3Object, Files.createTempFile(FilenameUtils.removeExtension(FilenameUtils.getBaseName(jar.key)), "jar").toFile)
      //  We're now tying the lifecycle of this file to the specific key that made the request
      tracker.track(tempFile, jar)
      //  Since we return the result with the key attached, we can guarantee that the file lives as long as the cached value
      LoadedJarFile(jar, tempFile)
    } finally IOUtils.closeQuietly(s3Object)
  }


  private def downloadToFile(s3Obj: S3Object, file: File) : File = {
    try {
      FileUtils.copyInputStreamToFile(s3Obj.getObjectContent, file)
    } catch {
      case e: Exception =>
        file.delete()
        throw e
    }
    file
  }
}

object JarLoadingActor {
  case class LoadJars(jars: Seq[RemoteStorageLocation])
  case class JarsResult(jars: Seq[LoadedJarFile])
}
