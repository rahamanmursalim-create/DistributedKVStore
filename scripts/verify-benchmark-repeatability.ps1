[CmdletBinding()]
param(
    [ValidateRange(1, 10)][int]$RunCount = 3,
    [ValidateRange(1, 256)][int]$TcpClients = 32,
    [ValidateRange(100, 1000000)][int]$TcpOperationsPerClient = 5000,
    [ValidateRange(0, 100000)][int]$TcpWarmupOpsPerClient = 1000,
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$ArtifactsRoot = (Join-Path (Split-Path -Parent $PSScriptRoot) 'artifacts'),
    [double]$MinimumTcpThroughputOpsSec = 0,
    [double]$MaximumTcpP99Ms = 0
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'validation-common.ps1')
$RepoRoot = (Resolve-Path $RepoRoot).Path
Assert-NoConcurrentMaven
$javaLine = (& java -version 2>&1 | Select-Object -First 1).ToString()
if ($javaLine -notmatch '"17(?:\.|"|-)') { throw "Use the verified Java 17 runtime. Current runtime: $javaLine" }
$mvnVersionLine = (& mvn -version 2>&1 | Select-Object -First 3) -join ' | '
if ($mvnVersionLine -notmatch 'Java version:\s*17(?:\.|$)') { throw "Maven does not appear to run on Java 17: $mvnVersionLine" }
$runId = 'bench-' + (Get-Date -Format 'yyyyMMdd-HHmmss')
$outDir = Join-Path $ArtifactsRoot $runId
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$started = Get-Date
$rows = [System.Collections.Generic.List[object]]::new()
$errors = 0

$compile = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments @('-q','-Daether.syncContext.named.factory=rwlock-local','clean','test-compile') -LogPath (Join-Path $outDir '00-test-compile.log') -TimeoutSeconds 900
if ($compile.ExitCode -ne 0 -or $compile.TimedOut) { throw "test-compile failed. See $($compile.LogPath)" }

$benchmarks = @(
    [pscustomobject]@{Name='single-node-tcp-resp'; Class='com.codecrafters.redis.TcpRespBenchmark'},
    [pscustomobject]@{Name='multi-raft-api'; Class='com.codecrafters.redis.raft.MultiRaftBenchmark'}
)
foreach ($bench in $benchmarks) {
    for ($run = 1; $run -le $RunCount; $run++) {
        Write-Host "Benchmark $($bench.Name), repetition $run/$RunCount"
        $arguments = @('-q','-Daether.syncContext.named.factory=rwlock-local')
        if ($bench.Name -eq 'single-node-tcp-resp') {
            $arguments += @("-Dbench.clients=$TcpClients", "-Dbench.operations=$TcpOperationsPerClient", "-Dbench.warmup=$TcpWarmupOpsPerClient")
        }
        $arguments += @(("-Dexec.mainClass=$($bench.Class)"),'-Dexec.classpathScope=test','org.codehaus.mojo:exec-maven-plugin:3.1.0:java')
        $logPath = Join-Path $outDir ("{0}-repeat-{1:D2}.log" -f $bench.Name, $run)
        $result = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments $arguments -LogPath $logPath -TimeoutSeconds 900
        if ($result.ExitCode -ne 0 -or $result.TimedOut) {
            $errors++
            $rows.Add([pscustomobject]@{Mode=$bench.Name; Repeat=$run; Subrun=''; Operations=0; ThroughputOpsSec=0; P50Ms=0; P95Ms=0; P99Ms=0; Errors=-1; ExitCode=$result.ExitCode; LogPath=$logPath})
            continue
        }
        $matches = [regex]::Matches($result.Output, '(?m)^run-(?<run>\d+):\s+(?:ops|operations)=(?<ops>\d+),\s+elapsedSec=(?<elapsed>[0-9.]+),\s+throughputOpsSec=(?<throughput>[0-9.]+),\s+p50Ms=(?<p50>[0-9.]+),\s+p95Ms=(?<p95>[0-9.]+),\s+p99Ms=(?<p99>[0-9.]+)(?:,\s+maxMs=[0-9.]+)?,\s+errors=(?<errs>\d+)')
        if ($matches.Count -eq 0) {
            $errors++
            $rows.Add([pscustomobject]@{Mode=$bench.Name; Repeat=$run; Subrun='parse-failure'; Operations=0; ThroughputOpsSec=0; P50Ms=0; P95Ms=0; P99Ms=0; Errors=-1; ExitCode=2; LogPath=$logPath})
            continue
        }
        foreach ($m in $matches) {
            $errCount = [int]$m.Groups['errs'].Value
            if ($errCount -ne 0) { $errors++ }
            $rows.Add([pscustomobject]@{
                Mode=$bench.Name; Repeat=$run; Subrun=('run-' + $m.Groups['run'].Value)
                Operations=[long]$m.Groups['ops'].Value; ThroughputOpsSec=[double]$m.Groups['throughput'].Value
                P50Ms=[double]$m.Groups['p50'].Value; P95Ms=[double]$m.Groups['p95'].Value; P99Ms=[double]$m.Groups['p99'].Value
                Errors=$errCount; ExitCode=0; LogPath=$logPath
            })
        }
    }
}

$tcpRows = @($rows | Where-Object { $_.Mode -eq 'single-node-tcp-resp' -and $_.ExitCode -eq 0 -and $_.Errors -eq 0 })
if ($MinimumTcpThroughputOpsSec -gt 0 -and $tcpRows.Count -gt 0) {
    $throughputs = @($tcpRows | ForEach-Object { [double]$_.ThroughputOpsSec } | Sort-Object)
    $middle = [int][Math]::Floor($throughputs.Count / 2)
    $medianThroughput = if (($throughputs.Count % 2) -eq 0) { ($throughputs[$middle - 1] + $throughputs[$middle]) / 2.0 } else { $throughputs[$middle] }
    if ($medianThroughput -lt $MinimumTcpThroughputOpsSec) { $errors++; Write-Warning "TCP benchmark median throughput $medianThroughput is below requested threshold $MinimumTcpThroughputOpsSec" }
}
if ($MaximumTcpP99Ms -gt 0 -and $tcpRows.Count -gt 0) {
    $maxP99 = ($tcpRows | Measure-Object -Property P99Ms -Maximum).Maximum
    if ($maxP99 -gt $MaximumTcpP99Ms) { $errors++; Write-Warning "Observed TCP P99 $maxP99 ms exceeds requested threshold $MaximumTcpP99Ms ms" }
}
$manifest = [ordered]@{
    runId=$runId; startedAt=$started.ToString('o'); finishedAt=(Get-Date).ToString('o')
    javaVersion=$javaLine; mavenVersion=((& mvn -version 2>&1 | Select-Object -First 3) -join ' | ')
    os=[System.Environment]::OSVersion.VersionString; processorCount=[Environment]::ProcessorCount
    totalRamMiB=$(try { [Math]::Round((Get-CimInstance Win32_ComputerSystem -ErrorAction Stop).TotalPhysicalMemory / 1MB, 0) } catch { $null })
    tcpClients=$TcpClients; tcpOperationsPerClient=$TcpOperationsPerClient; tcpWarmupOpsPerClient=$TcpWarmupOpsPerClient
    repeats=$RunCount; rows=@($rows); observedErrorCount=$errors
    thresholds=[ordered]@{minimumTcpThroughputOpsSec=$MinimumTcpThroughputOpsSec; maximumTcpP99Ms=$MaximumTcpP99Ms}
    note='Measurements are specific to this machine and workload; they are not automatically production SLOs.'
}
$rows | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $outDir 'benchmark-results.csv')
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 (Join-Path $outDir 'benchmark-results.json')

Write-Host "Benchmark evidence: $outDir"
if ($errors -gt 0) { exit 1 }
