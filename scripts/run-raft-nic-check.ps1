param(
    [Parameter(Mandatory = $true)][string]$HostAddress
)
$ErrorActionPreference = 'Stop'
$local = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
    Where-Object { $_.IPAddress -eq $HostAddress }
if (-not $local) { throw "$HostAddress is not assigned to a local IPv4 interface" }
& mvn -q test-compile
if ($LASTEXITCODE -ne 0) { throw "Maven test compilation failed with exit code $LASTEXITCODE" }
& mvn -q dependency:build-classpath "-Dmdep.outputFile=target/test-classpath.txt" -DincludeScope=test
if ($LASTEXITCODE -ne 0) { throw "Maven test classpath resolution failed with exit code $LASTEXITCODE" }
$java = (Get-Command java.exe -ErrorAction Stop).Source
$classpath = "target/test-classes;target/classes;" + (Get-Content target/test-classpath.txt -Raw)
$previous = $env:KV_TEST_HOST
$previousFailureMode = $env:KV_TEST_SKIP_FAILURE
try {
    $env:KV_TEST_HOST = $HostAddress
    $env:KV_TEST_SKIP_FAILURE = 'true'
    & $java -ea -cp $classpath com.codecrafters.redis.raft.RaftMultiProcessTest
    if ($LASTEXITCODE -ne 0) { throw "Raft interface test failed with exit code $LASTEXITCODE" }
} finally {
    $env:KV_TEST_HOST = $previous
    $env:KV_TEST_SKIP_FAILURE = $previousFailureMode
}
