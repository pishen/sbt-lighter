sys.props.get("plugin.version") match {
  case Some(x) =>
    addSbtPlugin("net.pishen" % "sbt-lighter" % x)
  case _ =>
    sys.error(
      """|The system property 'plugin.version' is not defined.
         |Specify this property using the scriptedLaunchOpts -D.
      """.stripMargin
    )
}

libraryDependencies += "org.scalamock" %% "scalamock" % "4.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4"
