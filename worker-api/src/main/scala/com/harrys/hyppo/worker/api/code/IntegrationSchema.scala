package com.harrys.hyppo.worker.api.code

import java.util

import org.apache.avro.Schema

/**
 * Created by jpetty on 7/30/15.
 */
@SerialVersionUID(1L)
final case class IntegrationSchema private(
  private val schemaString: String,
  fingerprint: Array[Byte]
) extends Serializable
{

  def this(schema: Schema)   = this(schema.toString(false), IntegrationUtils.computeSchemaFingerprint(schema))

  @transient lazy val schema = new Schema.Parser().parse(schemaString)

  def fingerprintHex: String = IntegrationUtils.fingerprintAsHex(fingerprint)

  def avroSchemaJson: String = schemaString

  def isSameSchema(other: IntegrationSchema) : Boolean = {
    schema.getName == other.schema.getName &&
    util.Arrays.equals(fingerprint, other.fingerprint)
  }

  def recordName: String = schema.getFullName
}


object IntegrationSchema {
  def apply(schema: Schema) : IntegrationSchema = {
    new IntegrationSchema(schema)
  }
}
