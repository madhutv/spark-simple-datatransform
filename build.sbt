import AssemblyKeys._

//sbt-assembly
assemblySettings

name := "SimpleDataTransformer"

scalaVersion := "2.11.8"

organization := "org.singaj"

mainClass := Some("UsageTest")

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.8.0",
  "io.circe" %% "circe-parser" % "0.8.0",
  "io.circe" %% "circe-generic" % "0.8.0",
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)


