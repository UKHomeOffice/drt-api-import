import Dependencies._

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val root = (project in file("."))
  .settings(
    name := "drt-api-import",
    resolvers += DefaultMavenRepository,
    libraryDependencies ++= Def.setting(Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.h2database" % "h2" % h2DatabaseVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,

      "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5",
      "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
      "org.codehaus.janino" % "janino" % janinoVersion,

      "com.typesafe" % "config" % typesafeConfigVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "software.amazon.awssdk" % "s3" % awsSdkS3Version,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-codegen" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "io.spray" %% "spray-json" % sprayJsonVersion,
      "org.postgresql" % "postgresql" % postgresqlVersion,
    )).value,
    Test / parallelExecution := false,
    Test / javaOptions += "-Duser.timezone=UTC",
//    coverageEnabled := true,
    coverageExcludedPackages := "<empty>;.*Main.*"
  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(AshScriptPlugin)
