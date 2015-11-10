package com.harrys.hyppo.config

import com.typesafe.config.Config

/**
 * Created by jpetty on 8/27/15.
 */
final class CoordinatorConfig(config: Config) extends HyppoConfig(config) {

  //  Location to store jar files
  val codeBucketName = config.getString("hyppo.code-bucket-name")

}
