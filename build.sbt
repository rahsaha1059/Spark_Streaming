name := "STBStreamApp1_6"

version := "0.0.1"

scalaVersion := "2.10.6"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.2" % "provided"
  //,"org.apache.spark" %% "spark-streaming-kafka" % "1.6.2" % "provided" 
)


//assemblyMergeStrategy in assembly := {

  //case PathList("META-INF",xs @ _*) => MergeStrategy.discard
 // case x => MergeStrategy.first
//}


