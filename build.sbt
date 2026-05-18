import net.nmoncho.sbt.dependencycheck.settings.{ AnalyzerSettings, NvdApiSettings }

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

addCommandAlias("scalafmtAll", "all scalafmtSbt scalafmt Test/scalafmt")

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

lazy val root = (project in file("."))
  .settings(
    name := "drt-api-import",
    libraryDependencies ++= AppDependencies.compileDependencies,
    libraryDependencies ++= AppDependencies.testDependencies,
    dockerBaseImage := "openjdk:11-jre-slim-buster"
  )
  .settings(CodeCoverageSettings.codeCoverageSettings)
  .settings(SbtUpdatesSettings.sbtUpdatesSettings)
  .settings(WartRemoverSettings.wartRemoverSettings)
  .enablePlugins(DockerPlugin)
  .enablePlugins(AshScriptPlugin)
