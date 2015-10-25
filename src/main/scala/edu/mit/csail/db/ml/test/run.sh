rm -rf project
rm -rf target
sbt package
spark-submit --master local target/scala-2.10/testapp_2.10-1.0.jar
