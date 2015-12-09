package com.harrys.hyppo.worker.api.code

import java.io.{File, FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.security.{DigestInputStream, MessageDigest}
import javax.xml.bind.DatatypeConverter

import org.apache.avro.Schema

/**
 * Created by jpetty on 7/30/15.
 */
object IntegrationUtils {

  private final val DigestType = "MD5"

  private def newDigest(): MessageDigest = MessageDigest.getInstance(DigestType)

  def computeFileFingerprint(file: File) : Array[Byte] = {
    val digest = newDigest()
    appendFileDigest(digest, file)
    digest.digest()
  }

  def computeSchemaFingerprint(schema: Schema) : Array[Byte] = {
    val digest = newDigest()
    appendSchemaFingerprint(digest, schema)
    digest.digest()
  }

  def wrapStreamForDigest(stream: InputStream): DigestInputStream = new DigestInputStream(stream, newDigest())

  def fingerprintAsHex(value: Array[Byte]) : String = DatatypeConverter.printHexBinary(value)

  def fingerprintHexAsBytes(value: String) : Array[Byte] = DatatypeConverter.parseHexBinary(value)

  private def appendSchemaFingerprint(digest: MessageDigest, schema: Schema) : Unit = {
    digest.update(schema.toString(false).getBytes(StandardCharsets.UTF_8))
  }

  private def appendFileDigest(digest: MessageDigest, source: File) : Unit = {
    val stream = new FileInputStream(source)
    try {
      val buffer: Array[Byte] = new Array[Byte](1024)
      var read: Int = stream.read(buffer)

      while(read != -1){
        digest.update(buffer, 0, read)
        read = stream.read(buffer)
      }
    } finally {
      stream.close()
    }
  }
}
