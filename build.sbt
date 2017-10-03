name := "sbt-emr-spark"

version := "0.12.0"

scalaVersion := "2.10.6"

sbtPlugin := true

val awsVersion = "1.11.195"
val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-emr" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-sts" % awsVersion,
  "com.github.eirslett" %% "sbt-slf4j" % "0.1",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.slf4s" %% "slf4s-api" % "1.7.12"
)

addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

publishMavenStyle := false
organization := "net.pishen"

enablePlugins(AutomateHeaderPlugin)
organizationName := "Pishen Tsai"
startYear := Some(2017)
licenses += ("Apache-2.0" -> url(
  "https://www.apache.org/licenses/LICENSE-2.0.html"))
