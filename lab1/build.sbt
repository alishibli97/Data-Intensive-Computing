name := "project1"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
// multiple dependencies
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.4.3",
"org.apache.spark" % "spark-streaming_2.11" % "2.4.3",
"org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.3",
"org.apache.spark" %% "spark-sql" % "2.4.3",

)
