package com.harrys.hyppo.worker.exec

import org.apache.avro.AvroRuntimeException
import org.apache.avro.file.CodecFactory

import scala.util.Try

/**
  * Created by jpetty on 12/31/15.
  */
final class AvroFileCodec(val name: String) {
  val factory = Try(CodecFactory.fromString(name)).recover {
    case e: AvroRuntimeException => throw new IllegalArgumentException(s"Invalid avro codec name: $name")
  }
}
