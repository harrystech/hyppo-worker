package com.harrys.hyppo.worker.api.code

import com.harrys.hyppo.worker.api.proto.RemoteStorageLocation

/**
 * Created by jpetty on 8/3/15.
 */
@SerialVersionUID(1L)
final case class IntegrationCode
(
  integrationClass: String,
  jarFiles:         Seq[RemoteStorageLocation]
) extends Serializable
{

  def isSameCode(other: IntegrationCode) : Boolean = {
    integrationClass == other.integrationClass &&
    jarFiles.size == other.jarFiles.size &&
    jarFiles.zip(other.jarFiles).forall(pair => pair._1.isSameLocation(pair._2))
  }
}
