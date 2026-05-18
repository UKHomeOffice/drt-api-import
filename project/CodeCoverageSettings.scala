import sbt._
import sbt.Keys._
import scoverage.ScoverageKeys.coverageExcludedPackages

object CodeCoverageSettings {
  val codeCoverageSettings: Seq[Def.Setting[_]] = Seq(
    Test / parallelExecution := false,
    Test / javaOptions += "-Duser.timezone=UTC",
    coverageExcludedPackages := "<empty>;.*Main.*"
  )
}
