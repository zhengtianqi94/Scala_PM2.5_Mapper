name := "ScalaFinalProject"

version := "1.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "3.0.1",
  "co.theasi" %% "plotly" % "0.2.0",
  "org.scala-lang" % "scala-swing" % "2.11.0-M7",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"
)