ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "P2"
  )
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
//libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.12.0"

// Below two lines are needed for Justis to be able to run the program without IntelliJ...
fork in run := true
connectInput in run := true

/*
Compile / mainClass := Some("P2")
assembly / mainClass := Some("P2")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

 */



