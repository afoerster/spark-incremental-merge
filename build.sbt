name := "sparkTesting"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.spark" %% "spark-hive" % "2.1.0"

)