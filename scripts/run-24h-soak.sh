#!/usr/bin/env bash
set -uo pipefail

duration_hours="${1:-24}"
artifact_root="${2:-$PWD/artifacts}"
if [[ ! "$duration_hours" =~ ^[1-9][0-9]*$ ]] || (( duration_hours > 168 )); then
  echo "Duration must be a whole number from 1 through 168 hours." >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$artifact_root"
artifact_root="$(cd "$artifact_root" && pwd)"
run_id="kvstore-soak-$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="$artifact_root/$run_id"
mkdir -p "$run_dir"

# A lock prevents a second soak from running Maven against this checkout.
exec 9>"$repo_root/.kvstore-soak-maven.lock"
if ! flock -n 9; then
  echo "Another soak owns the Maven lock in $repo_root" >&2
  exit 3
fi

java_line="$(java -version 2>&1 | head -n 1)"
maven_version="$(mvn -version 2>&1 | head -n 4)"
if ! grep -Eq 'version "17([.]|")' <<< "$java_line"; then
  echo "This soak requires Java 17; found: $java_line" >&2
  exit 4
fi
if ! grep -Eq 'Java version: 17([.]|$)' <<< "$maven_version"; then
  echo "Maven must run on Java 17. Found:\n$maven_version" >&2
  exit 4
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to verify generated class-file versions." >&2
  exit 4
fi
python3 - "$repo_root/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

pom = ET.parse(sys.argv[1]).getroot()
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
plugins = pom.findall("m:build/m:plugins/m:plugin", ns)
compiler = next((p for p in plugins if p.findtext("m:artifactId", namespaces=ns) == "maven-compiler-plugin"), None)
if compiler is None or compiler.findtext("m:configuration/m:release", namespaces=ns) != "17":
    raise SystemExit("maven-compiler-plugin must set <release>17</release>")
surefire = next((p for p in plugins if p.findtext("m:artifactId", namespaces=ns) == "maven-surefire-plugin"), None)
if surefire is None or surefire.findtext("m:configuration/m:skip", namespaces=ns) == "true":
    raise SystemExit("Maven Surefire must exist and must not be skipped")
junit_present = any(
    dependency.findtext("m:groupId", namespaces=ns) == "org.junit.jupiter"
    and dependency.findtext("m:artifactId", namespaces=ns) == "junit-jupiter"
    and dependency.findtext("m:scope", namespaces=ns) == "test"
    for dependency in pom.findall("m:dependencies/m:dependency", ns)
)
if not junit_present:
    raise SystemExit("JUnit Jupiter test dependency is missing")
print("Verified POM: compiler release 17, Surefire enabled, JUnit Jupiter present.")
PY
config_code=$?
if (( config_code != 0 )); then exit 4; fi

started_epoch="$(date +%s)"
deadline_epoch=$(( started_epoch + duration_hours * 3600 ))
started_utc="$(date -u --iso-8601=seconds)"
cycle=0
failed=0
interrupted=0

cat > "$run_dir/environment.txt" <<EOF
runId=$run_id
startedAtUtc=$started_utc
requestedDurationHours=$duration_hours
repoRoot=$repo_root
gitCommit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unavailable)
java=$java_line
maven=$maven_version
expectedClassFileMajor=$((44 + 17))
host=$(hostname -f 2>/dev/null || hostname)
kernel=$(uname -a)
EOF
printf 'cycle,startedAtUtc,finishedAtUtc,elapsedSeconds,exitCode,status,log\n' > "$run_dir/results.csv"

write_summary() {
  local status="$1" now elapsed
  now="$(date +%s)"
  elapsed=$(( now - started_epoch ))
  cat > "$run_dir/summary.txt.tmp" <<EOF
runId=$run_id
status=$status
startedAtUtc=$started_utc
finishedAtUtc=$(date -u --iso-8601=seconds)
requestedDurationHours=$duration_hours
elapsedSeconds=$elapsed
durationSatisfied=$([[ $elapsed -ge $((duration_hours * 3600)) ]] && echo true || echo false)
cyclesCompleted=$cycle
anyCycleFailed=$([[ $failed -eq 1 ]] && echo true || echo false)
note=Every cycle invokes the complete Maven clean test suite. PASS requires the requested elapsed time and zero failed cycles.
EOF
  mv "$run_dir/summary.txt.tmp" "$run_dir/summary.txt"
}

handle_signal() { interrupted=1; }
trap handle_signal INT TERM
write_summary RUNNING

while (( $(date +%s) < deadline_epoch )); do
  if (( interrupted )); then
    write_summary INCOMPLETE
    exit 130
  fi
  cycle=$((cycle + 1))
  cycle_start="$(date -u --iso-8601=seconds)"
  cycle_start_epoch="$(date +%s)"
  log_file="$run_dir/cycle-$(printf '%04d' "$cycle")-mvn-clean-test.log"
  echo "Cycle $cycle started at $cycle_start"
  if mvn -B "-Daether.syncContext.named.factory=rwlock-local" clean test > "$log_file" 2>&1; then
    if python3 "$repo_root/scripts/check-classfile-version.py" \
      "$repo_root/target/classes" "$repo_root/target/test-classes" >> "$log_file" 2>&1; then
      code=0
      status=PASS
    else
      code=$?
      status=FAIL
      failed=1
    fi
  else
    code=$?
    status=FAIL
    failed=1
  fi
  cycle_end="$(date -u --iso-8601=seconds)"
  cycle_elapsed=$(( $(date +%s) - cycle_start_epoch ))
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$cycle" "$cycle_start" "$cycle_end" "$cycle_elapsed" "$code" "$status" "$(basename "$log_file")" >> "$run_dir/results.csv"
  write_summary RUNNING
  echo "Cycle $cycle $status (exit $code, ${cycle_elapsed}s)"
  if (( interrupted )); then
    write_summary INCOMPLETE
    exit 130
  fi
  if (( code != 0 )); then
    write_summary FAILED
    exit 1
  fi
done

# The last full suite may extend past the deadline; it must finish successfully.
if (( failed == 0 && cycle > 0 && $(date +%s) - started_epoch >= duration_hours * 3600 )); then
  write_summary PASS
  exit 0
fi
write_summary INCOMPLETE
exit 1
