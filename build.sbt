name := "ML Project"

version := "1.0"

scalaVersion := "2.10.4"

test in assembly := {}
assemblyJarName in assembly := "ml.jar"

// unmanagedBase := baseDirectory.value / "lib"

// libraryDependencies += "org.apache.commons" % "commons-math3" % "3.4.1"
// libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.1"
// libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "1.5.1"
// libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.5.1"
// libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.1"
// libraryDependencies += "org.jpmml" % "pmml-model" % "1.1.15"
// libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.11.2"


// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.2" % "provided"

libraryDependencies += "org.eclipse.jetty" % "jetty-webapp" % "9.1.0.v20131115"
libraryDependencies += "org.eclipse.jetty" % "jetty-plus" % "9.1.0.v20131115"
libraryDependencies += "org.eclipse.jetty" % "jetty-client" % "9.2.10.v20150310"

libraryDependencies += "org.mongodb" %% "casbah" % "3.0.0"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
