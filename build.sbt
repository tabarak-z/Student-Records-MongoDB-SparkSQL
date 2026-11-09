
name := "HW2-MongoDB-SparkSQL"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.1",
  "org.apache.spark" %% "spark-sql" % "3.5.3"
)

Compile / unmanagedSourceDirectories += baseDirectory.value
Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"