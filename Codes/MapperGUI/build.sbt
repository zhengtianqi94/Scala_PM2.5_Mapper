name := "ScalaFinalProject"

version := "1.0.0"

scalaVersion := "2.10.4"

val scalaTestVersion = "2.2.4"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.scala-lang" % "scala-swing" % "2.10+"
)