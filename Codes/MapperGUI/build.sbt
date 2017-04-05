name := "ScalaFinalProject"

version := "1.0.0"

scalaVersion := "2.10.4"

val scalaTestVersion = "2.2.4"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.scala-lang" % "scala-swing" % "2.10+",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0"
)