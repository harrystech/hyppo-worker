package com.harrys.hyppo.worker.actor.task

/**
 * Created by jpetty on 10/29/15.
 */
final case class ResultLogData(subTasksTotal: Int, resultsSent: Int = 0, logsUploaded: Int = 0) {

  def newSentResult: ResultLogData = this.copy(resultsSent = this.resultsSent + 1)

  def newUploadedLog: ResultLogData = this.copy(logsUploaded = this.logsUploaded + 1)

  def pendingLogFiles: Int = subTasksTotal - logsUploaded

  def pendingResults: Int = subTasksTotal - resultsSent

  def pendingUploads: Int = pendingResults - pendingLogFiles

  def hasMoreResultsPending: Boolean = pendingResults > 0

  def hasMoreLogsPending: Boolean = pendingLogFiles > 0

  def inspect: String = {
    s"total=${ subTasksTotal } resultsSent=${ resultsSent } logsUploaded=${ logsUploaded }"
  }
}
