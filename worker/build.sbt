name := "worker"

val AkkaVersion = "2.3.13"

val GuiceVersion = "4.0"

resolvers += "The New Motion Public Repo" at "http://nexus.thenewmotion.com/content/groups/public/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"  % AkkaVersion,         // Akka actors
  "org.json4s"        %% "json4s-jackson"  % "3.3.0",         // json4s - scala json interface wrapping jackson
  "commons-io"        %  "commons-io"      % "2.4",           // Handy utils for IO stuff
  "ch.qos.logback"    %  "logback-classic" % "1.1.3",         // Logging API implementation
  "io.dropwizard.metrics" % "metrics-core" % "3.1.0",         // Metrics Reporting framework
  "com.google.code.findbugs" % "jsr305" % "3.0.0",            // Compile time checks based on annotations
  "com.thenewmotion.akka" %% "akka-rabbitmq" % "1.2.4",       // RabbitMQ client for work negotiation
  "org.apache.httpcomponents" % "httpclient" % "4.5",         // HTTP client for RabbitMQ management API
  "com.amazonaws" %  "aws-java-sdk-s3"  % "1.10.54",          // Reading / writing to S3
  "org.joda"      % "joda-convert"      % "1.8",              // Necessary since aws client pulls in joda time without this dependency and the compiler complains
  "com.google.inject" % "guice"         % GuiceVersion,       // Dependency injection
  "com.google.inject.extensions" % "guice-assistedinject" % GuiceVersion,
  "com.sandinh" %% "akka-guice" % "3.1.1" excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0" exclude("org.scala-lang", "scala-reflect")  // scala logging API
)

mainClass in run := Some("com.harrys.hyppo.WorkerMain")

val configFile = (resourceDirectory in Test).map { _  / "hyppo-test.conf" }

javaOptions ++= Seq(s"-Dconfig.file=${ configFile.value.toString }")

fork in run := true

// --
//  Testing Setup
//--

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "org.scalacheck" %% "scalacheck" % "1.12.5" % Test,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
  "org.mockito" % "mockito-core" % "1.10.19" % Test
)

//  Set the classpath so we can fork a new JVM
testOptions in Test += Tests.Setup(() => {
  System.setProperty("testing.classpath", (fullClasspath in Test).value.files.map(_.getAbsolutePath).mkString(":"))
})

javaOptions in Test += "-Dtesting.classpath=" + (fullClasspath in Test).value.files.map(_.getAbsolutePath).mkString(":")

//  Setup the frameworks explicitly to keep ScalaCheck from running separately
testFrameworks in Test := Seq(TestFrameworks.ScalaTest)

exportJars := true

// --
// Console Testing Setup
// --

initialCommands in (console in Test) :=
  s"""
    |import akka.actor._
    |import com.harrys.hyppo.HyppoWorker
    |import com.harrys.hyppo.worker._
    |import com.harrys.hyppo.config._
    |import com.typesafe.config._
    |import com.harrys.hyppo.worker.actor.amqp._
    |import com.harrys.hyppo.worker.actor._
    |import java.io.File
    |import java.util.UUID
    |import java.time._
  """.stripMargin

