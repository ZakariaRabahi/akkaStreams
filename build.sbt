ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "taak1"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.8.6",
  "com.typesafe.akka" %% "akka-actor" % "2.8.6",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "6.0.2",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.8.6" % Test
)

