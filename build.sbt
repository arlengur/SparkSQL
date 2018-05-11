lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  name := "SparkSQL",
  version := "0.0.1",
  scalaVersion := "2.11.8",
  scalacOptions ++= List("-unchecked", "-deprecation", "-encoding", "UTF8", "-feature")
)

lazy val root = {
  project in file(".")
}.settings(commonSettings).settings(
  libraryDependencies ++= dependencies
)

val sparkVersion = "2.3.0"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.6.0-M3"
)