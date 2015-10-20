package com.harrys.hyppo

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.duration._


/**
 * Created by jpetty on 7/29/15.
 */
object WorkerMain {
  final val log = Logger(LoggerFactory.getLogger(this.getClass))

  def main(args: Array[String]) : Unit = {
    val appConfig =
      if (System.getProperty("config.file") != null){
        val location = System.getProperty("config.file")
        log.info(s"Using configuration file from property 'config.file': $location")
        ConfigFactory.parseFile(new File(location)).withFallback(ConfigFactory.defaultApplication())
      } else if (System.getProperty("config.resource") != null) {
        val location = System.getProperty("config.resource")
        log.info(s"Using configuration classpath resource from property 'config.resource': $location")
        ConfigFactory.parseResources(location)
      } else {
        log.info("No configuration file passed. Using default application location.")
        ConfigFactory.defaultApplication()
      }

    val fullConfig = appConfig
      .withFallback(HyppoWorker.referenceConfig())
      .resolve()

    log.debug(s"Configuration: \n${ fullConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(true).setFormatted(true)) }")

    val worker = HyppoWorker(ActorSystem("hyppo", fullConfig))

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run() : Unit = {
        if (!worker.system.isTerminated){
          worker.system.shutdown()
          log.info("Waiting for akka system shutdown...")
          worker.system.awaitTermination(Duration(8, SECONDS))
        }
        log.info("ActorSystem shutdown complete")
      }
    }))

    worker.system.awaitTermination()
  }
}
