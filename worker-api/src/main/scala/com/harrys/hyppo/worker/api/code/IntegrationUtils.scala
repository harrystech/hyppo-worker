package com.harrys.hyppo.worker.api.code

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

import org.apache.avro.Schema

/**
 * Created by jpetty on 7/30/15.
 */
object IntegrationUtils {
  val DigestType = "MD5"

  def computeFileFingerprint(jar: File) : Array[Byte] = {
    val digest = createMessageDigest()
    appendFileDigest(digest, jar)
    digest.digest()
  }

  def computeSchemaFingerprint(schema: Schema) : Array[Byte] = {
    val digest = createMessageDigest()
    appendSchemaFingerprint(digest, schema)
    digest.digest()
  }

  def fingerprintAsHex(value: Array[Byte]) : String = DatatypeConverter.printHexBinary(value)

  def fingerprintHexAsBytes(value: String) : Array[Byte] = DatatypeConverter.parseHexBinary(value)

  private def createMessageDigest() : MessageDigest = MessageDigest.getInstance(DigestType)

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
