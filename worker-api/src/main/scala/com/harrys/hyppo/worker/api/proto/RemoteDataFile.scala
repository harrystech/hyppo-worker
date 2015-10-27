package com.harrys.hyppo.worker.api.proto

/**
 * Created by jpetty on 8/4/15.
 */
sealed trait RemoteDataFile extends Product with Serializable {
  def bucket: String
  def key: String
}

@SerialVersionUID(1L)
final case class RemoteRawDataFile
(
  override val bucket: String,
  override val key: String,
  fileSize: Long
) extends RemoteDataFile

@SerialVersionUID(1L)
final case class RemoteProcessedDataFile
(
  override val  bucket: String,
  override val  key: String,
  recordCount:  Long
) extends RemoteDataFile

@SerialVersionUID(1L)
final case class RemoteLogFile
(
  override val bucket: String,
  override val key: String
) extends RemoteDataFile
