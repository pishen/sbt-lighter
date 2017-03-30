name := "sbt-emr-spark"



scalaVersion := "2.10.6"

sbtPlugin := true

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-emr" % "1.11.76",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.76",
  "com.github.pathikrit" %% "better-files" % "2.14.0",
  "com.github.eirslett" %% "sbt-slf4j" % "0.1",
  "com.typesafe.play" %% "play-json" % "2.4.8",
  "org.slf4s" %% "slf4s-api" % "1.7.12"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

publishMavenStyle := false
organization := "net.pishen"
licenses += ("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html"))
