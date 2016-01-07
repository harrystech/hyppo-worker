import sbt.Keys._
import sbt._

object HyppoWorker {
  //  This is the version of the canonical source-api that all components will be built against
  final val ApiVersion    = "0.6.4"

  //  This is the version that all components will share when published
  final val WorkerVersion = "0.6.5-SNAPSHOT"

  lazy val universalSettings = Seq(
    organization := "com.harrys.hyppo",
    version := WorkerVersion,
    libraryDependencies += "com.harrys.hyppo" % "source-api" % ApiVersion,
    resolvers ++= Seq(Resolver.sonatypeRepo("public"), Resolver.sonatypeRepo("snapshots"))
  ) ++ publishSettings

  lazy val scalaSettings = Seq(
    scalaVersion := "2.11.7",
    scalacOptions ++= DefaultOptions.scalac,
    scalacOptions in (Compile, compile) ++= Seq("-deprecation", "–unchecked", "-feature", "-Xlint"),
    scalacOptions in (Compile, doc) := DefaultOptions.scalac,
    ivyScala in Compile := (ivyScala in Compile).value.map(_.copy(overrideScalaVersion = true))
  ) ++ universalSettings

  lazy val publishSettings = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ =>  false },
    licenses := Seq("The MIT License (MIT)" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/harrystech/hyppo-worker")),
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := {
      <scm>
        <url>git@github.com:harrystech/hyppo-worker.git</url>
        <connection>scm:git:git@github.com:harrystech/hyppo-worker.git</connection>
      </scm>
      <developers>
        <developer>
          <id>pettyjamesm</id>
          <name>James Petty</name>
        </developer>
      </developers>
    }
  )
}