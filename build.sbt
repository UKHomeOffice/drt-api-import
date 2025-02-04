import Dependencies._

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val root = (project in file("."))
  .settings(
    name := "drt-api-import",
    resolvers ++= Seq(
      "Akka library repository".at("https://repo.akka.io/maven"),
    ),
    libraryDependencies ++= Def.setting(Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.h2database" % "h2" % h2DatabaseVersion % Test,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-pki" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,

      "ch.qos.logback.contrib" % "logback-json-classic" % logbackContribVersion,
      "ch.qos.logback.contrib" % "logback-jackson" % logbackContribVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
      "org.codehaus.janino" % "janino" % janinoVersion,

      "com.typesafe" % "config" % typesafeConfigVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "software.amazon.awssdk" % "s3" % awsSdkS3Version,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-codegen" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "org.postgresql" % "postgresql" % postgresqlVersion,
      "com.github.gphat" %% "censorinus" % censorinusVersion,
    )).value,
    Test / parallelExecution := false,
    Test / javaOptions += "-Duser.timezone=UTC",
//    coverageEnabled := true,
    coverageExcludedPackages := "<empty>;.*Main.*",

    dockerBaseImage := "openjdk:11-jre-slim-buster",
  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(AshScriptPlugin)
