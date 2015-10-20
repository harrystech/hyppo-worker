package com.harrys.hyppo.worker.api.code

/**
 * Created by jpetty on 7/30/15.
 */
@SerialVersionUID(1L)
final case class IntegrationJarFile(bucketName: String, objectKey: String) extends Serializable {

  def isSameJarFile(other: IntegrationJarFile) : Boolean = {
    bucketName == other.bucketName &&
    objectKey  == other.objectKey
  }

}
