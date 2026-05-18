#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

sbt clean scalafmtAll scalafmtSbt compile coverage test coverageOff coverageReport dependencyUpdates


