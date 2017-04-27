name := "ScalaFinalProject"

version := "1.0.0"

scalaVersion := "2.10.4"

val scalaTestVersion = "2.2.4"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.scala-lang" % "scala-swing" % "2.10+",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0",
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.0.0"
)