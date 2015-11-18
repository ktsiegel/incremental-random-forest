name := "ML Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"

libraryDependencies += "org.eclipse.jetty" % "jetty-webapp" % "9.1.0.v20131115"
libraryDependencies += "org.eclipse.jetty" % "jetty-plus" % "9.1.0.v20131115"

libraryDependencies += "org.mongodb" %% "casbah" % "3.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.+" % "test"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
