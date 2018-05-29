name := "Simple Project"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.scala-saddle" %% "saddle-core" % "1.3.+"
)