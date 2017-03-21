name := "PM2.5Mapper"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.0.2",
  "com.github.tototoshi" %% "scala-csv" % "1.3.1",
  "co.theasi" %% "plotly" % "0.2.0"
)