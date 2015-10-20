package com.harrys.hyppo.worker.api.code

import com.harrys.hyppo.source.api.model.IngestionSource
import com.typesafe.config.Config

/**
 * Created by jpetty on 7/29/15.
 */
@SerialVersionUID(1L)
final case class ExecutableIntegration
(
  source:         IngestionSource,
  schema:         IntegrationSchema,
  code:           IntegrationCode,
  details:        IntegrationDetails
) extends Serializable
{

  def sourceName: String = source.getName

  def sourceConfig: Config = source.getConfiguration

  def isSameCodeBase(other: ExecutableIntegration) : Boolean = {
    sourceName == other.sourceName &&
    code.isSameCode(other.code) &&
    details.isSameDetail(other.details) &&
    schema.isSameSchema(other.schema)
  }

  def printableName: String = {
    s"${this.productPrefix}(source=$sourceName avroType=${schema.recordName} version=${details.versionNumber})"
  }
}
