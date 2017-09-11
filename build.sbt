name := "sbt-emr-spark"

version := "0.10.0"

scalaVersion := "2.10.6"

sbtPlugin := true

val awsVersion = "1.11.134"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-emr" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.github.eirslett" %% "sbt-slf4j" % "0.1",
  "org.slf4s" %% "slf4s-api" % "1.7.12"
)

enablePlugins(AutomateHeaderPlugin)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

publishMavenStyle := false
organization := "net.pishen"
startYear := Some(2017)
licenses += ("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html"))
