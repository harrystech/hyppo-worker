package com.harrys.hyppo.worker.data

import java.io.File

import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

/**
 * Created by jpetty on 7/30/15.
 */
final case class LoadedJarFile(key: RemoteStorageLocation, file: File)
