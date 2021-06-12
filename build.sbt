scalaVersion := "2.11.12"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0"
)

libraryDependencies += "org" % "graphframes" % "0.7.0-spark2.4-s_2.11" from "file:///app/lib/graphframes-0.7.0-spark2.4-s_2.11.jar"

// set the main class for packaging the main jar
mainClass in (Compile, packageBin) := Some("com.wrapper.BoruvkaAlgorithm")

// set the main class for the main 'sbt run' task
mainClass in (Compile, run) := Some("com.wrapper.BoruvkaAlgorithm")