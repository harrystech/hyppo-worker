package com.harrys.hyppo.worker.api.code

import com.harrys.hyppo.source.api.model.IngestionSource
import com.typesafe.config.Config

/**
 * Created by jpetty on 7/30/15.
 */
@SerialVersionUID(1L)
final case class UnvalidatedIntegration
(
  source:  IngestionSource,
  code:    IntegrationCode,
  version: Int
) extends Serializable
{

  def sourceName: String   = source.getName

  def sourceConfig: Config = source.getConfiguration

  def isSameCodeBase(other: UnvalidatedIntegration) : Boolean = {
    sourceName == other.sourceName && code.isSameCode(other.code)
  }
}
