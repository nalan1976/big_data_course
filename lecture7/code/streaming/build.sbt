name := "MyStreaming"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.3"

scalacOptions ++= Seq("-no-specialization")
