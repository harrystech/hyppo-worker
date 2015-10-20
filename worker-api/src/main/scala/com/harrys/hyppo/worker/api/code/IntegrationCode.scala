package com.harrys.hyppo.worker.api.code

/**
 * Created by jpetty on 8/3/15.
 */
@SerialVersionUID(1L)
final case class IntegrationCode
(
  integrationClass: String,
  jarFiles:         Seq[IntegrationJarFile]
) extends Serializable
{

  def isSameCode(other: IntegrationCode) : Boolean = {
    integrationClass == other.integrationClass &&
    jarFiles.size == other.jarFiles.size &&
    jarFiles.zip(other.jarFiles).forall(pair => pair._1.isSameJarFile(pair._2))
  }
}
