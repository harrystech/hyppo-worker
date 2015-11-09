package com.harrys.hyppo.worker.actor.amqp

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

/**
  * Created by jpetty on 11/9/15.
  */
final class LookaheadObjectInputStream(classes: Set[Class[_]], inputStream: InputStream) extends ObjectInputStream(inputStream) {

  private var lookupMap: Map[Class[_], Class[_]] = classes.map(c => c -> c).toMap

  @throws[ClassNotFoundException]("if no associated class can be found")
  @throws[UnsupportedOperationException]("if the identified class is not valid for the declared serialization classes")
  override protected def resolveClass(objectStreamClass: ObjectStreamClass) : Class[_] = {
    val resolved = super.resolveClass(objectStreamClass)
    if (lookupMap.contains(resolved)){
      return resolved
    } else {
      classes.find(check => check.isAssignableFrom(check)) match {
        case None =>
          throw new UnsupportedOperationException(s"Illegal serialization of class: ${ resolved.getName }. Supported objects must sub-class one of (${ classes.toSeq.map(_.getName).mkString(",") })")
        case Some(found) =>
          lookupMap += (resolved -> found)
          resolved
      }
    }
  }
}
