import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  lazy val typesafeConfig = "com.typesafe" % "config" % "1.3.2"
  lazy val akkaStream = "com.typesafe.akka" %% "akka-stream" % "2.5.22"
  lazy val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.22" % Test
  lazy val awsJavaSdkCore = "com.amazonaws" % "aws-java-sdk-core" % "1.11.534"
  lazy val commonsAwsS3 = "com.mfglabs" %% "commons-aws-s3" % "0.12.2"
  lazy val slick = "com.typesafe.slick" %% "slick" % "3.2.3"
  lazy val slickCodegen = "com.typesafe.slick" %% "slick-codegen" % "3.2.3"
  lazy val slickHirakiCp = "com.typesafe.slick" %% "slick-hikaricp" % "3.2.3"
  lazy val sprayJson = "io.spray" %%  "spray-json" % "1.3.5"
  lazy val postgres = "org.postgresql" % "postgresql" % "42.2.5"
  lazy val h2 = "com.h2database" % "h2" % "1.4.199" % Test
}
