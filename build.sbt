name := "ML Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.+" % "test"
libraryDependencies += "org.mongodb" %% "casbah" % "3.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
