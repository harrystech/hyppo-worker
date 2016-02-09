package com.harrys.hyppo.worker.data

import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

import scala.concurrent.Future

/**
  * Created by jpetty on 2/9/16.
  */
trait JarFileLoader {

  def shutdown(): Unit

  def loadJarFile(jar: RemoteStorageLocation): Future[LoadedJarFile]

}
