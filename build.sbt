scalaVersion := "2.11.12"

sparkVersion := "2.4.0"
sparkComponents ++= Seq("sql")
sparkComponents ++= Seq("graphx")

spDependencies += "graphframes/graphframes:0.7.0-spark2.4-s_2.11"

// set the main class for packaging the main jar
mainClass in (Compile, packageBin) := Some("com.wrapper.BoruvkaAlgorithm")

// set the main class for the main 'sbt run' task
mainClass in (Compile, run) := Some("com.wrapper.BoruvkaAlgorithm")