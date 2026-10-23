param(
    [ValidateRange(1, 100000)][int]$Operations = 5000,
    [ValidateRange(1, 1048576)][int]$ValueBytes = 128
)
$ErrorActionPreference = 'Stop'
& mvn -q test-compile
if ($LASTEXITCODE -ne 0) { throw "Maven test compilation failed with exit code $LASTEXITCODE" }
& mvn -q exec:java "-Dexec.mainClass=com.codecrafters.redis.raft.RaftBenchmark" `
    "-Dexec.classpathScope=test" "-Dexec.args=$Operations $ValueBytes"
if ($LASTEXITCODE -ne 0) { throw "Raft benchmark failed with exit code $LASTEXITCODE" }
