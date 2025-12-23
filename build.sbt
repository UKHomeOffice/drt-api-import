import Dependencies.*
import net.nmoncho.sbt.dependencycheck.settings.{AnalyzerSettings, NvdApiSettings}

ThisBuild / scalaVersion := "2.13.15"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val root = (project in file("."))
  .settings(
    name := "drt-api-import",
    libraryDependencies ++= Def.setting(Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.h2database" % "h2" % h2DatabaseVersion % Test,
      "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-pki" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion % Test,
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,

      "ch.qos.logback" % "logback-classic" % logbackVersion,
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
      "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
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

val nvdAPIKey = sys.env.getOrElse("NVD_API_KEY", "")

ThisBuild / dependencyCheckNvdApi := NvdApiSettings(apiKey = nvdAPIKey)

ThisBuild / dependencyCheckAnalyzers := dependencyCheckAnalyzers.value.copy(
  ossIndex = AnalyzerSettings.OssIndex(
    enabled = Some(false),
    url = None,
    batchSize = None,
    requestDelay = None,
    useCache = None,
    warnOnlyOnRemoteErrors = None,
    username = None,
    password = None
  )
)