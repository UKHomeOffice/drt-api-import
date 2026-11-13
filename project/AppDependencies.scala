import sbt.*

object AppDependencies {
  private val pekkoVersion = "1.7.1"
  private val pekkoHttpVersion = "1.4.2"
  private val slickVersion = "3.6.1"

  val compileDependencies: Seq[ModuleID] = Seq(
    "org.apache.pekko"           %% "pekko-stream"          % pekkoVersion,
    "org.apache.pekko"           %% "pekko-http"            % pekkoHttpVersion,
    "ch.qos.logback"              % "logback-classic"       % "1.6.7",
    "ch.qos.logback.contrib"      % "logback-json-classic"  % "0.1.5",
    "ch.qos.logback.contrib"      % "logback-jackson"       % "0.1.5",
    "com.fasterxml.jackson.core"  % "jackson-databind"      % "2.21.8",
    "com.typesafe"                % "config"                % "1.4.10",
    "com.typesafe.scala-logging" %% "scala-logging"         % "3.9.6",
    "software.amazon.awssdk"      % "s3"                    % "2.57.13",
    "joda-time"                   % "joda-time"             % "2.15.1",
    "com.typesafe.slick"         %% "slick"                 % slickVersion,
    "com.typesafe.slick"         %% "slick-hikaricp"        % slickVersion,
    "org.apache.pekko"           %% "pekko-http-spray-json" % pekkoHttpVersion,
    "org.postgresql"              % "postgresql"            % "42.7.14",
    "com.github.gphat"           %% "censorinus"            % "2.1.16"
  )

  val testDependencies: Seq[ModuleID] = Seq(
    "org.scalatest"    %% "scalatest"            % "3.2.20"         % Test,
    "com.h2database"    % "h2"                   % "2.5.252"        % Test,
    "org.apache.pekko" %% "pekko-http-testkit"   % pekkoHttpVersion % Test,
    "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion     % Test
  )
}
