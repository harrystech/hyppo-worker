package com.harrys.hyppo.worker.api.proto

/**
 * Created by jpetty on 8/4/15.
 */
sealed trait RemoteDataFile extends Product with Serializable {
  def location: RemoteStorageLocation
  def fileSize: Long
  def checkSum: Array[Byte]
}

@SerialVersionUID(1L)
final case class RemoteRawDataFile
(
  override val location:  RemoteStorageLocation,
  override val fileSize:  Long,
  override val checkSum:  Array[Byte]
) extends RemoteDataFile

@SerialVersionUID(1L)
final case class RemoteProcessedDataFile
(
  override val  location: RemoteStorageLocation,
  override val  fileSize: Long,
  override val  checkSum: Array[Byte],
  recordCount:  Long
) extends RemoteDataFile

@SerialVersionUID(1L)
final case class RemoteLogFile
(
  override val location: RemoteStorageLocation,
  override val fileSize:  Long,
  override val checkSum:  Array[Byte]
) extends RemoteDataFile
