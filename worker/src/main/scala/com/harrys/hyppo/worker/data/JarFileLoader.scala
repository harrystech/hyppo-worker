package com.harrys.hyppo.worker.data

import com.google.inject.assistedinject.Assisted
import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by jpetty on 2/9/16.
  */
trait JarFileLoader {

  def shutdown(): Unit

  def loadJarFile(jar: RemoteStorageLocation): Future[LoadedJarFile]

}

object JarFileLoader {
  trait Factory {
    def apply(@Assisted executionContext: ExecutionContext): JarFileLoader
  }
}
