name := "sparkMerge"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0.cloudera1"

resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "3.1.2"
)
