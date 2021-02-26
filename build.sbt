name := "EIPR"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided,
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.4.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "mysql" % "mysql-connector-java" % "8.0.23",
  "com.typesafe" % "config" % "1.2.1"
)

test in assembly := {}
