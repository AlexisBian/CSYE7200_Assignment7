name := "assignment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.10"

idePackagePrefix := Some("org.example")

val sparkVersion = "3.1.1"
val scalaTestVersion = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)