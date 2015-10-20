package com.harrys.hyppo.worker.actor.amqp

import akka.actor.{ActorContext, ActorSystem}
import akka.serialization.{Serialization, SerializationExtension}
import org.apache.commons.lang3.SerializationException

import scala.reflect._
import scala.util.{Failure, Success}

/**
 * Created by jpetty on 9/16/15.
 */
final class AMQPSerialization(serialization: Serialization) {

  def this(system: ActorSystem)   = this(SerializationExtension(system))

  def this(context: ActorContext) = this(context.system)

  def serialize(o: AnyRef) : Array[Byte] = {
    serialization.serialize(o) match {
      case Success(bytes) => bytes
      case Failure(cause) =>
        throw new SerializationException(s"Failed to serialize instance of ${o.getClass.getCanonicalName}", cause)
    }
  }

  def deserialize[T : ClassTag](bytes: Array[Byte]) : T = {
    val rtClass = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    deserialize(bytes, rtClass)
  }

  def deserialize[T](bytes: Array[Byte], klass: Class[T]) : T = {
    serialization.deserialize(bytes, klass) match {
      case Success(ready) => ready.asInstanceOf[T]
      case Failure(cause) =>
        throw new SerializationException(s"Failed to deserialize bytes into instance of ${klass.getCanonicalName}", cause)
    }
  }
}
