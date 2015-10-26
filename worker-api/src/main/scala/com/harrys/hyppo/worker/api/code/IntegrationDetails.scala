package com.harrys.hyppo.worker.api.code

import com.harrys.hyppo.source.api.PersistingSemantics

/**
 * Created by jpetty on 7/30/15.
 */
@SerialVersionUID(1L)
final case class IntegrationDetails
(
  isRawDataIntegration: Boolean,
  persistingSemantics: PersistingSemantics,
  versionNumber: Int
) extends Serializable
{

def isProcessedDataIntegration: Boolean = !this.isRawDataIntegration

  def isSameDetail(other: IntegrationDetails) : Boolean = {
    versionNumber == other.versionNumber &&
    isRawDataIntegration == other.isRawDataIntegration &&
    persistingSemantics == other.persistingSemantics
  }
}
