import AssemblyKeys._

//sbt-assembly
assemblySettings

name := "SimpleDataTransformer"

scalaVersion := "2.11.8"

organization := "org.singaj"

mainClass := Some("UsageTest")

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-native" % "3.5.2",
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)


