//  Pull in shared publishing configuration with all universal settings
HyppoWorker.publishSettings

//  As the aggregate, this project needs to root Sonatype organization def
organization := "com.harrys"

//  Just for making IntelliJ happy. This project has no sources outside of SBT
name := "hyppo-worker"

//  This project is only for aggregating the others
publishArtifact := false

//  Executor launcher implementation
lazy val executor = project.in(file("executor")).
  settings(HyppoWorker.universalSettings)

//  Coordinator Process / Worker communication
lazy val workerApi = project.in(file("worker-api")).
  settings(HyppoWorker.scalaSettings)

//  Worker-side coordinating Process
lazy val worker = project.in(file("worker")).
  settings(HyppoWorker.scalaSettings).
  dependsOn(
    executor,
    workerApi
  )

lazy val root = project.in(file(".")).
  aggregate(
    executor,
    workerApi,
    worker
  )


