package com.harrys.hyppo.config

import com.typesafe.config.{ConfigValueFactory, ConfigValue, Config}

/**
 * Created by jpetty on 8/27/15.
 */
final class CoordinatorConfig(config: Config) extends HyppoConfig(config) {

  //  Location to store jar files
  val codeBucketName = config.getString("hyppo.code-bucket-name")


  def withValue(path: String, value: ConfigValue): CoordinatorConfig = {
    new CoordinatorConfig(underlying.withValue(path, value))
  }

  def withValue(path: String, value: String): CoordinatorConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value))
  }

  def withValue(path: String, value: Int): CoordinatorConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Integer]))
  }

  def withValue(path: String, value: Boolean): CoordinatorConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Boolean]))
  }

  def withValue(path: String, value: Double): CoordinatorConfig = {
    withValue(path, ConfigValueFactory.fromAnyRef(value.asInstanceOf[java.lang.Double]))
  }
}
