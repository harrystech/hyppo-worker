package com.harrys.hyppo.worker.data

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

import scala.concurrent.Future

final class JarLoadingActor @Inject() (loader: JarFileLoader) extends Actor with ActorLogging {
  import JarLoadingActor._
  //  Load the dispatcher as the default execution context
  import context.dispatcher

  override def postStop(): Unit = {
    loader.shutdown()
    super.postStop()
  }

  override def receive: Receive = {
    case LoadJars(jarFiles) =>
      loadJarFiles(jarFiles, sender())
  }

  def loadJarFiles(jarFiles: Seq[RemoteStorageLocation], recipient: ActorRef) : Unit = {
    val downloads = jarFiles.map(jar => loader.loadJarFile(jar))
    val combined  = Future.sequence(downloads).map(jars => JarsResult(jars))
    combined.pipeTo(recipient)
  }
}

object JarLoadingActor {
  case class LoadJars(jars: Seq[RemoteStorageLocation])
  case class JarsResult(jars: Seq[LoadedJarFile])
}
