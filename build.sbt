name := "spark csv demo"

organization := "com.octo.spark"

version := "1.0-SNAPSHOT"

description := "spark csv demo"

publishMavenStyle := false

crossPaths := false

autoScalaLibrary := false

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "1.5.1",
   "org.apache.spark" %% "spark-sql" % "1.5.1",
   "com.databricks" %% "spark-csv" % "1.2.0"
)
