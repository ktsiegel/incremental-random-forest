name := "ML Project"

version := "1.0"

scalaVersion := "2.11.7"

test in assembly := {}
assemblyJarName in assembly := "ml.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"

libraryDependencies += "org.eclipse.jetty" % "jetty-webapp" % "9.1.0.v20131115"
libraryDependencies += "org.eclipse.jetty" % "jetty-plus" % "9.1.0.v20131115"
libraryDependencies += "org.eclipse.jetty" % "jetty-client" % "9.2.10.v20150310"

libraryDependencies += "org.mongodb" %% "casbah" % "3.0.0"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
