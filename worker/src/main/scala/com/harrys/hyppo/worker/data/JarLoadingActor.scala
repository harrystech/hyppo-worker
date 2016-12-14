package com.harrys.hyppo.worker.data

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

import scala.concurrent.Future
import scala.util.{Failure, Success}

final class JarLoadingActor @Inject() (factory: JarFileLoader.Factory) extends Actor with ActorLogging {
  import JarLoadingActor._
  //  Load the dispatcher as the default execution context
  import context.dispatcher

  val loader: JarFileLoader = factory(context.dispatcher)

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
    combined.onComplete {
      case Success(result) =>
        recipient ! result
      case f @ Failure(c) =>
        log.warning(s"Failed to download jars from ${jarFiles.mkString(", ")}", c)
        recipient ! f
    }
  }
}

object JarLoadingActor {
  final case class LoadJars(jars: Seq[RemoteStorageLocation])
  final case class JarsResult(jars: Seq[LoadedJarFile])
}
