package com.harrys.hyppo

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Await
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

    //  This had better not work.
    ConfigFactory.invalidateCaches()

    val config = HyppoWorker.createConfig(appConfig)

    if (config.printConfiguration){
      log.info(s"Configuration: \n${ config.underlying.root().render(ConfigRenderOptions.defaults().setOriginComments(true).setFormatted(true)) }")
    }

    val system = ActorSystem("hyppo", config.underlying)
    //  Instantiate the workers and start processing
    val worker = HyppoWorker(system, config)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run() : Unit = {
        val terminated = system.terminate()
        log.info("Waiting for akka system shutdown...")
        worker.shutdownActorSystem(Duration(8, SECONDS))
        log.info("ActorSystem shutdown complete")
      }
    }))

    // Wait for shutdown
    worker.suspendUntilSystemTermination()
  }
}
