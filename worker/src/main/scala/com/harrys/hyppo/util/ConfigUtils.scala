package com.harrys.hyppo.util

import java.io.Reader
import java.net.InetAddress

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.IOUtils

import scala.io.Source

/**
 * Created by jpetty on 8/28/15.
 */
object ConfigUtils {

  def resourceFileConfig(path: String) : Config = withResourceReader(path) { reader =>
    ConfigFactory.parseReader(reader)
  }

  def withResourceReader[T](path: String)(action: Reader => T) : T = {
    val reader = Source.fromInputStream(this.getClass.getResourceAsStream(path)).reader()
    try {
      action(reader)
    } finally {
      IOUtils.closeQuietly(reader)
    }
  }

  def localHostname: String = InetAddress.getLocalHost.getCanonicalHostName
}
