ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.0")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.12")
addDependencyTreePlugin
