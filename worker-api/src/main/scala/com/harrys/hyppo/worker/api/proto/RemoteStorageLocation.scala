package com.harrys.hyppo.worker.api.proto

/**
  * Created by jpetty on 12/9/15.
  */
@SerialVersionUID(1L)
final case class RemoteStorageLocation(bucket: String, key: String) extends Serializable {

  def isSameLocation(other: RemoteStorageLocation): Boolean = {
    bucket == other.bucket && key == other.key
  }
}
