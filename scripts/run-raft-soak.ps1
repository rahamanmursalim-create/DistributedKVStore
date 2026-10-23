param(
    [ValidateRange(1, 10080)][int]$DurationMinutes = 60,
    [ValidateRange(0, 5000)][int]$ForceDelayMillis = 0,
    [string]$ArtifactDirectory = "artifacts/raft-soak-$(Get-Date -Format yyyyMMdd-HHmmss)"
)
$ErrorActionPreference = 'Stop'
& mvn -q test-compile
if ($LASTEXITCODE -ne 0) { throw "Maven test compilation failed with exit code $LASTEXITCODE" }
& mvn -q dependency:build-classpath "-Dmdep.outputFile=target/test-classpath.txt" -DincludeScope=test
if ($LASTEXITCODE -ne 0) { throw "Maven test classpath resolution failed with exit code $LASTEXITCODE" }
$java = (Get-Command java.exe -ErrorAction Stop).Source
$classpath = "target/test-classes;target/classes;" + (Get-Content target/test-classpath.txt -Raw)
$artifactPath = [System.IO.Path]::GetFullPath($ArtifactDirectory)
New-Item -ItemType Directory -Force -Path $artifactPath | Out-Null
$logPath = Join-Path $artifactPath 'soak.log'
$summaryPath = Join-Path $artifactPath 'summary.json'
$deadline = (Get-Date).AddMinutes($DurationMinutes)
$iteration = 0
$started = Get-Date
$completed = $false
$failed = $false
$previousDelay = $env:KV_TEST_FORCE_DELAY_MS
try {
    if ($ForceDelayMillis -gt 0) { $env:KV_TEST_FORCE_DELAY_MS = "$ForceDelayMillis" }
    while ((Get-Date) -lt $deadline) {
        $iteration++
        $stamp = Get-Date -Format o
        Write-Host "[$stamp] Raft multiprocess failover/recovery soak iteration $iteration"
        "[$stamp] iteration=$iteration forceDelayMs=$ForceDelayMillis" | Add-Content -LiteralPath $logPath
        $stdoutPath = Join-Path $artifactPath 'iteration.stdout.tmp'
        $stderrPath = Join-Path $artifactPath 'iteration.stderr.tmp'
        $arguments = @()
        if ($ForceDelayMillis -gt 0) { $arguments += "-Dkv.test.forceDelayMillis=$ForceDelayMillis" }
        $arguments += @('-ea','-cp',$classpath,'com.codecrafters.redis.raft.RaftMultiProcessTest')
        $process = Start-Process -FilePath $java -ArgumentList $arguments -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath -NoNewWindow -PassThru -Wait
        $exitCode = $process.ExitCode
        if (Test-Path -LiteralPath $stdoutPath) { Get-Content -LiteralPath $stdoutPath | Add-Content -LiteralPath $logPath; Remove-Item -LiteralPath $stdoutPath -Force }
        if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath | Add-Content -LiteralPath $logPath; Remove-Item -LiteralPath $stderrPath -Force }
        if ($exitCode -ne 0) { $failed = $true; throw "Soak iteration $iteration failed with exit code $exitCode" }
    }
    $completed = $true
    Write-Host "Soak completed: $iteration iterations over $DurationMinutes minute(s)."
} finally {
    $env:KV_TEST_FORCE_DELAY_MS = $previousDelay
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $javaVersion = (& $java -version 2>&1 | ForEach-Object { $_.ToString() }) -join ' '
    $ErrorActionPreference = $previousPreference
    [ordered]@{
        startedAt = $started.ToString('o'); finishedAt = (Get-Date).ToString('o')
        requestedMinutes = $DurationMinutes; iterations = $iteration; forceDelayMillis = $ForceDelayMillis
        result = if ($failed) { 'FAIL' } elseif ($completed) { 'PASS' } else { 'INTERRUPTED' }; java = $javaVersion
        machine = $env:COMPUTERNAME; log = 'soak.log'
    } | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $summaryPath
    Write-Host "Soak artifacts: $artifactPath"
}
