import sbt._

object Dependencies {
  private val scalatestVersion = "3.2.11"
  private val logbackClassicVersion = "1.2.10"
  private val typesafeConfigVersion = "1.4.2"
  private val akkaVersion = "2.6.18"
  private val slickVersion = "3.3.3"
  private val sprayJsonVersion = "1.3.6"
  private val postgresqlVersion = "42.3.3"
  private val h2DatabaseVersion = "2.1.210"
  private val scalaLoggingVersion = "3.9.4"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalatestVersion
  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % logbackClassicVersion
  lazy val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
  lazy val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
  lazy val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
  lazy val awsJavaSdkCore = "com.amazonaws" % "aws-java-sdk-core" % "1.11.534"
  lazy val commonsAwsS3 = "com.mfglabs" %% "commons-aws-s3" % "0.12.2"
  lazy val slick = "com.typesafe.slick" %% "slick" % slickVersion
  lazy val slickCodegen = "com.typesafe.slick" %% "slick-codegen" % slickVersion
  lazy val slickHirakiCp = "com.typesafe.slick" %% "slick-hikaricp" % slickVersion
  lazy val sprayJson = "io.spray" %%  "spray-json" % sprayJsonVersion
  lazy val postgres = "org.postgresql" % "postgresql" % postgresqlVersion
  lazy val h2 = "com.h2database" % "h2" % h2DatabaseVersion % Test
}
