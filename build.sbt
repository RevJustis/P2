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

// Below two lines are needed for Justis to be able to run the program without IntelliJ...
fork in run := true
connectInput in run := true

//This gives a new name for the jar file so we can find it easier in the target/scala folder
assemblyJarName in assembly := "P2.jar"
//When using sbt-assembly we can encounter errors caused by the default deduplicate merge strategy.
//In most cases this is caused by files in the META-INF directory.
//This code resolves merge issues by choosing the appropriate merge strategy for the paths
//that are causing errors.
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}






