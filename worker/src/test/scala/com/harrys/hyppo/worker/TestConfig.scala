package com.harrys.hyppo.worker

import com.harrys.hyppo.HyppoWorker
import com.harrys.hyppo.util.ConfigUtils
import com.typesafe.config.Config

/**
 * Created by jpetty on 8/28/15.
 */
object TestConfig {


  def basicTestConfig: Config = {
    ConfigUtils.resourceFileConfig("/hyppo-test.conf")
      .withFallback(HyppoWorker.referenceConfig())
      .resolve()
  }
}
