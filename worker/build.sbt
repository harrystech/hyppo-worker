name := "worker"

val AkkaVersion = "2.3.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"  % AkkaVersion,     // Akka actors
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",  // scala logging API
  "joda-time"         %  "joda-time"       % "2.8.1",         // better date / time APIs
  "com.google.guava"  %   "guava"          % "18.0",          //  Caches / Other Utils
  "org.json4s"        %% "json4s-jackson"  % "3.2.11",        // json4s - scala json interface wrapping jackson
  "org.json4s"        %% "json4s-ext"      % "3.2.11",        // json4s <> joda-time extensions
  "commons-io"        %  "commons-io"      % "2.4",           // Handy utils for IO stuff
  "ch.qos.logback"    %  "logback-classic" % "1.1.3",         // Logging API implementation
  "com.google.code.findbugs" % "jsr305" % "3.0.0",            // Compile time checks based on annotations
  "com.github.sstone" %% "amqp-client" % "1.5",               // RabbitMQ client for work negotiation
  "org.apache.httpcomponents" % "httpclient" % "4.5",          // HTTP client for RabbitMQ management API
  "javax.inject" % "javax.inject" % "1",
  "com.amazonaws" %  "aws-java-sdk-s3" % "1.10.2"        // Reading / writing to S3
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
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
  "org.mockito" % "mockito-core" % "1.10.19" % Test
)

//  Set the classpath so we can fork a new JVM
testOptions in Test += Tests.Setup(() => {
  System.setProperty("testing.classpath", (fullClasspath in Test).value.files.map(_.getAbsolutePath).mkString(":"))
})

//  Setup the J-Unit arguments for testing
testOptions += Tests.Argument(TestFrameworks.ScalaTest)

// --
// Console Testing Setup
// --

initialCommands in console :=
  """
    |import akka.actor._
    |import com.harrys.hyppo.HyppoWorker
    |import com.harrys.hyppo.worker._
    |import com.harrys.hyppo.config._
    |import com.typesafe.config._
    |import com.harrys.hyppo.worker.actor.amqp._
    |import java.io.File
  """.stripMargin

