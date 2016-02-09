package com.harrys.hyppo.worker.data

import java.io.File

import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

/**
  * This pairing may seem unnecessary at first, but the combination of this local file reference and [[RemoteStorageLocation]]
  * allows [[org.apache.commons.io.FileCleaningTracker]] to detect when the last reference to the [[RemoteStorageLocation]]
  * has been garbage collected and can therefore delete the local [[File]] instance from disk.
  *
  * By combining them into this case class and using the case class in the API signature, it ensures that consumers
  * don't retain a reference to the local file without also retaining a reference to the [[RemoteStorageLocation]] instance.
  *
  * Created by jpetty on 7/30/15.
  */
final case class LoadedJarFile(key: RemoteStorageLocation, file: File)
