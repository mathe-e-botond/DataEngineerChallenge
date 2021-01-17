name := "web-log-analytics"
version := "0.0.0-SNAPSHOT"
organization := "com.mathebotond"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "com.github.scopt" %% "scopt"      % "4.0.0"      % Compile,
  "org.scalatest"    %% "scalatest"  % "3.2.2"      % "test, it"
)

// test run settings
//parallelExecution in Test := false
assembly / test := {}

// Enable integration tests
Defaults.itSettings
lazy val root = project.in(file(".")).configs(IntegrationTest)

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")

// Publish settings
//publishTo := Some("Sonatype Snapshots Nexus" at "https://oss.sonatype.org/content/repositories/snapshots")
//publishTo := {
//  val nexus = "https://my.artifact.repo.net/"
//  if (isSnapshot.value)
//    Some("snapshots" at nexus + "content/repositories/snapshots")
//  else
//    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
//}
//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")