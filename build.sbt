name := "sbt-emr-spark"

version := "0.8.1"

scalaVersion := "2.10.6"

sbtPlugin := true

val awsVersion = "1.11.134"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-emr" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.github.pathikrit" %% "better-files" % "2.14.0",
  "com.github.eirslett" %% "sbt-slf4j" % "0.1",
  "com.typesafe.play" %% "play-json" % "2.4.8",
  "org.slf4s" %% "slf4s-api" % "1.7.12"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

publishMavenStyle := false
organization := "net.pishen"
licenses += ("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html"))
