name := "executor"

//  Don't export scala as a dependency (it's only for testing)
autoScalaLibrary := false

//  Don't inject scala version number into artifacts
crossPaths := false

//  Export jars instead of exporting the classpath location
exportJars := true

// --
//  Testing Setup
//--

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "commons-io" % "commons-io" % "2.4" % Test
)

//  Setup the J-Unit arguments for testing
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q")

