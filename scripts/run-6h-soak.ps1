param(
    [ValidateRange(1, 6)][int]$DurationHours = 6,
    [ValidateRange(1, 64)][int]$Concurrency = 1,
    [ValidateRange(0.1, 1000)][double]$TargetOpsPerSecond = 1.0,
    [Parameter(Mandatory=$true)][string]$ProjectPath,
    [string]$ArtifactRoot = 'artifacts',
    [string]$MavenRepo = 'C:\Users\mursa\AppData\Local\Temp\DistributedKVStore-m2-10h-20261101-090127',
    [switch]$PreflightOnly
)
$ErrorActionPreference = 'Stop'
$startedAt = Get-Date
$runId = 'soak-6h-' + $startedAt.ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$runDir = [System.IO.Path]::GetFullPath((Join-Path $ArtifactRoot $runId))
$project = [System.IO.Path]::GetFullPath($ProjectPath)
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$resultsPath = Join-Path $runDir 'cycles.csv'
$resourcePath = Join-Path $runDir 'resource-samples.csv'
$stopFile = Join-Path $runDir 'STOP_WORKLOAD'
$serverProcesses = @()
$workloadProcess = $null
$mavenProcess = $null
$status = 'PRECHECK'
$deadline = $null
$workloadStarted = $false
$previousEnv = @{}
$javaHome = 'C:\Users\mursa\AppData\Local\Programs\Temurin17\PFiles64\Eclipse Adoptium\jdk-17.0.20.101-hotspot'
$maven = (Get-Command mvn.cmd -ErrorAction Stop).Source
$python = (Get-Command python.exe -ErrorAction Stop).Source
$powershell = (Get-Command powershell.exe -ErrorAction Stop).Source
$workloadScript = Join-Path $PSScriptRoot 'soak-workload.py'
$workloadWrapper = Join-Path $PSScriptRoot 'invoke-soak-workload.ps1'
$cycleWrapper = Join-Path $PSScriptRoot 'invoke-soak-cycle.ps1'
$metricsSummarizer = Join-Path $PSScriptRoot 'summarize-soak-metrics.py'

function Save-Summary([string]$result) {
    $finished = Get-Date
    $elapsed = if ($deadline) { [math]::Round(($finished - $startedAt).TotalSeconds, 3) } else { 0 }
    $rows = if (Test-Path $resultsPath) { @(Import-Csv $resultsPath) } else { @() }
    $summary = [ordered]@{
        runId = $runId; status = $result; startedAtLocal = $startedAt.ToString('o')
        finishedAtLocal = $finished.ToString('o'); elapsedSeconds = $elapsed
        requestedDurationHours = $DurationHours; fullDurationSatisfied = ($result -eq 'PASS' -and $elapsed -ge $DurationHours * 3600)
        cycleCount = $rows.Count; passedCycles = @($rows | Where-Object status -eq 'PASS').Count
        failedCycles = @($rows | Where-Object status -eq 'FAIL').Count
        projectPath = $project; javaHome = $javaHome; maven = $maven; concurrency = $Concurrency; targetOpsPerSecond = $TargetOpsPerSecond
        cyclesCsv = 'cycles.csv'; resourceSamplesCsv = 'resource-samples.csv'
        performanceSummary = 'workload/performance-summary.json'
        consolidatedMetrics = 'final-metrics-report.json'
        evidenceNote = 'A failed, interrupted, or shorter-than-requested run is not PASS.'
    }
    $summary | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 -LiteralPath (Join-Path $runDir 'summary.json')
}

function Add-ResourceSample {
    $now = Get-Date
    $javaProcesses = @(Get-Process -Name java -ErrorAction SilentlyContinue | Where-Object {
        try { $_.Path -and $_.Path.StartsWith($javaHome, [StringComparison]::OrdinalIgnoreCase) } catch { $false }
    })
    $cpu = ($javaProcesses | Measure-Object -Property CPU -Sum).Sum
    $private = ($javaProcesses | Measure-Object -Property PrivateMemorySize64 -Sum).Sum
    $working = ($javaProcesses | Measure-Object -Property WorkingSet64 -Sum).Sum
    $heap = 0.0; $gcParts = @(); $jstatErrors = @()
    foreach ($server in $serverProcesses) {
        try {
            $sample = @(& "$javaHome\bin\jstat.exe" -gc $server.Id 2>&1)
            if ($LASTEXITCODE -ne 0 -or $sample.Count -lt 2) { throw (($sample -join ' ') + " exit=$LASTEXITCODE") }
            $headers = $sample[-2].Trim() -split '\s+'
            $values = $sample[-1].Trim() -split '\s+'
            $map = @{}
            for ($i = 0; $i -lt [math]::Min($headers.Count,$values.Count); $i++) { $map[$headers[$i]] = $values[$i] }
            $heap += [double]$map.S0U + [double]$map.S1U + [double]$map.EU + [double]$map.OU
            $gcParts += "n$($server.Id):YGC=$($map.YGC);YGCT=$($map.YGCT);FGC=$($map.FGC);FGCT=$($map.FGCT);GCT=$($map.GCT)"
        } catch { $jstatErrors += "pid=$($server.Id):$($_.Exception.Message)" }
    }
    if ($gcParts.Count -eq 0) { $heap = ''; $gc = '' } else { $gc = $gcParts -join '|' }
    $jstatError = $jstatErrors -join ' | '
    $diskRead = ''; $diskWrite = ''; $network = ''; $counterError = ''
    try {
        $counters = Get-Counter '\PhysicalDisk(_Total)\Disk Read Bytes/sec','\PhysicalDisk(_Total)\Disk Write Bytes/sec','\Network Interface(*)\Bytes Total/sec' -ErrorAction Stop
        $diskRead = ($counters.CounterSamples | Where-Object Path -Match 'disk read bytes/sec' | Measure-Object CookedValue -Sum).Sum
        $diskWrite = ($counters.CounterSamples | Where-Object Path -Match 'disk write bytes/sec' | Measure-Object CookedValue -Sum).Sum
        $network = ($counters.CounterSamples | Where-Object Path -Match 'bytes total/sec' | Measure-Object CookedValue -Sum).Sum
    } catch { $counterError = $_.Exception.Message }
    $drive = Get-PSDrive -Name ([System.IO.Path]::GetPathRoot($runDir).Substring(0,1)) -ErrorAction SilentlyContinue
    [pscustomobject]@{
        timestampLocal=$now.ToString('o'); javaProcessCount=$javaProcesses.Count
        javaCpuSeconds=$cpu; javaCpuByPid=(($javaProcesses | ForEach-Object { "$($_.Id):$($_.CPU)" }) -join ';')
        javaPrivateBytes=$private; javaWorkingSetBytes=$working
        jvmHeapUsedKb=$heap; gcCounters=$gc; jstatError=$jstatError
        hostDiskReadBytesPerSecond=$diskRead; hostDiskWriteBytesPerSecond=$diskWrite
        hostNetworkBytesPerSecond=$network; hostFreeDiskBytes=$drive.Free
        counterError=$counterError
    } | Export-Csv -LiteralPath $resourcePath -NoTypeInformation -Append
}

try {
    if (-not (Test-Path (Join-Path $project 'pom.xml'))) { throw "Project path has no pom.xml: $project" }
    if (-not (Test-Path $javaHome)) { throw "Java 17 installation missing: $javaHome" }
    if (-not (Test-Path $MavenRepo)) { throw "Private Maven cache missing: $MavenRepo" }
    foreach ($name in @('JAVA_HOME','PATH','MAVEN_OPTS','TCP_PORT','PORT','DATA_DIR','KV_NODE_ID','KV_RAFT_GROUP_ID','KV_RAFT_BIND_HOST','KV_RAFT_PORT','KV_RAFT_PEERS')) {
        $previousEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    $env:JAVA_HOME = $javaHome
    $env:Path = "$javaHome\bin;$env:Path"
    $env:MAVEN_OPTS = "-Dmaven.repo.local=$($MavenRepo.Replace('\','/'))"

    $versionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $javaInfo = (& "$javaHome\bin\java.exe" -version 2>&1 | ForEach-Object ToString) -join ' '
    $mavenInfo = (& $maven -version 2>&1 | ForEach-Object ToString) -join ' '
    $ErrorActionPreference = $versionPreference
    if ($javaInfo -notmatch '17\.0\.' -or $mavenInfo -notmatch 'Java version: 17\.') { throw "Java/Maven must both run Java 17. java=$javaInfo maven=$mavenInfo" }

    @("runId=$runId","requestedDurationHours=$DurationHours","host=$env:COMPUTERNAME","os=$([Environment]::OSVersion.VersionString)",
      "logicalProcessors=$([Environment]::ProcessorCount)","processor=$env:PROCESSOR_IDENTIFIER","java=$javaInfo","maven=$mavenInfo",
      "project=$project","concurrency=$Concurrency","targetOpsPerSecond=$TargetOpsPerSecond","workload=3-node local Multi-Raft, equal PUT/GET/DELETE over RESP") |
        Set-Content -Encoding utf8 -LiteralPath (Join-Path $runDir 'environment.txt')

    Push-Location $project
    try {
        & $maven -B clean test *> (Join-Path $runDir 'baseline-mvn-clean-test.log')
        if ($LASTEXITCODE -ne 0) { throw "Baseline full suite failed with exit code $LASTEXITCODE" }
        & $python scripts/check-classfile-version.py target/classes target/test-classes *> (Join-Path $runDir 'baseline-classfile-check.log')
        if ($LASTEXITCODE -ne 0) { throw "Baseline class-file verification failed with exit code $LASTEXITCODE" }
        & $maven -B -DskipTests package *> (Join-Path $runDir 'application-package.log')
        if ($LASTEXITCODE -ne 0) { throw "Application package failed with exit code $LASTEXITCODE" }
        & $python scripts/check-classfile-version.py target/classes *> (Join-Path $runDir 'package-classfile-check.log')
        if ($LASTEXITCODE -ne 0) { throw "Packaged application class-file verification failed with exit code $LASTEXITCODE" }
    } finally { Pop-Location }

    $jarSource = Join-Path $project 'target\redis-java-clone-1.0.0.jar'
    if (-not (Test-Path $jarSource)) { throw "Expected shaded application JAR missing: $jarSource" }
    $jar = Join-Path $runDir 'redis-java-clone-1.0.0.jar'
    Copy-Item -Force $jarSource $jar
    $env:KV_RAFT_PEERS = 'n1=127.0.0.1:17001,n2=127.0.0.1:17002,n3=127.0.0.1:17003'
    $baseClientPort = 16381; $baseWebPort = 18081; $baseRaftPort = 17001
    for ($i=1; $i -le 3; $i++) {
        $env:KV_NODE_ID = "n$i"; $env:KV_RAFT_GROUP_ID = 'soak'; $env:KV_RAFT_BIND_HOST = '127.0.0.1'
        $env:KV_RAFT_PORT = [string]($baseRaftPort + $i - 1); $env:TCP_PORT = [string]($baseClientPort + $i - 1)
        $env:PORT = [string]($baseWebPort + $i - 1); $env:DATA_DIR = Join-Path $runDir "node-$i-data"
        New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null
        $outLog = Join-Path $runDir "node-$i.stdout.log"; $errLog = Join-Path $runDir "node-$i.stderr.log"
        $serverProcesses += Start-Process -FilePath "$javaHome\bin\java.exe" -ArgumentList @('-jar', "`"$jar`"") `
            -WorkingDirectory $runDir -RedirectStandardOutput $outLog -RedirectStandardError $errLog -WindowStyle Hidden -PassThru
    }
    $env:KV_NODE_ID = $null; $env:KV_RAFT_GROUP_ID = $null; $env:KV_RAFT_BIND_HOST = $null; $env:KV_RAFT_PORT = $null
    $env:TCP_PORT = $null; $env:PORT = $null; $env:DATA_DIR = $null
    $readyArgs = @($workloadScript,'--port',[string]$baseClientPort,'--output',(Join-Path $runDir 'workload'),
                   '--stop-file',$stopFile,'--check')
    $ready = $false; $stableChecks = 0; $readyDeadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $readyDeadline -and -not $ready) {
        if (@($serverProcesses | Where-Object HasExited).Count -gt 0) { throw 'A Raft application node exited during startup; inspect node logs.' }
        & $python @readyArgs *> (Join-Path $runDir 'readiness-check.log')
        if ($LASTEXITCODE -eq 0) { $stableChecks++ } else { $stableChecks = 0 }
        "$(Get-Date -Format o),exit=$LASTEXITCODE,consecutiveSuccesses=$stableChecks" | Add-Content -LiteralPath (Join-Path $runDir 'readiness-history.csv')
        $ready = ($stableChecks -ge 10)
        if (-not $ready) { Start-Sleep -Seconds 1 }
    }
    if (-not $ready) { throw 'Three-node cluster did not pass ten consecutive end-to-end readiness checks within 180 seconds.' }
    Start-Sleep -Seconds 10

    New-Item -ItemType Directory -Force -Path (Join-Path $runDir 'workload') | Out-Null
    $workloadProcess = Start-Process -FilePath $powershell -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$workloadWrapper,
        '-PythonPath',$python,'-WorkloadPath',$workloadScript,'-Port',[string]$baseClientPort,
        '-Concurrency',[string]$Concurrency,'-TargetOpsPerSecond',[string]$TargetOpsPerSecond,
        '-OutputPath',(Join-Path $runDir 'workload'),'-StopFile',$stopFile,'-CyclesFile',$resultsPath,
        '-ExitCodePath',(Join-Path $runDir 'workload-exit-code.txt')) `
        -WorkingDirectory $project -RedirectStandardOutput (Join-Path $runDir 'workload.stdout.log') `
        -RedirectStandardError (Join-Path $runDir 'workload.stderr.log') -WindowStyle Hidden -PassThru
    $workloadStarted = $true
    if ($PreflightOnly) {
        $preflightCycleLog = Join-Path $runDir 'preflight-concurrent-mvn-clean-test.log'
        $preflightCycleCodeFile = Join-Path $runDir 'preflight-concurrent-test-exit-code.txt'
        $preflightCycle = Start-Process -FilePath $powershell -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$cycleWrapper,
            '-ProjectPath',$project,'-MavenPath',$maven,'-LogPath',$preflightCycleLog,'-ExitCodePath',$preflightCycleCodeFile) -WorkingDirectory $project `
            -RedirectStandardOutput (Join-Path $runDir 'preflight-concurrent-runner.stdout.log') `
            -RedirectStandardError (Join-Path $runDir 'preflight-concurrent-runner.stderr.log') -WindowStyle Hidden -PassThru
        if (-not $preflightCycle.WaitForExit(1200000)) {
            Stop-Process -Id $preflightCycle.Id -Force -ErrorAction SilentlyContinue
            throw 'Concurrent preflight test suite exceeded its 20-minute hang limit.'
        }
        $preflightCycle.Refresh()
        $preflightTestExit = if (Test-Path $preflightCycleCodeFile) { [int](Get-Content $preflightCycleCodeFile -Raw).Trim() } else { 1 }
        Start-Sleep -Seconds 30
        New-Item -ItemType File -Force -Path $stopFile | Out-Null
        $workloadProcess.WaitForExit(15000) | Out-Null
        $workloadProcess.Refresh()
        $workloadCodeFile = Join-Path $runDir 'workload-exit-code.txt'
        $preflightExit = if (Test-Path $workloadCodeFile) { [int](Get-Content $workloadCodeFile -Raw).Trim() } else { 1 }
        $preflightReport = Join-Path $runDir 'workload\performance-summary.json'
        $preflightStderr = Join-Path $runDir 'workload.stderr.log'
        $preflightFailed = ($preflightTestExit -ne 0) -or (-not $workloadProcess.HasExited) -or (-not (Test-Path $preflightReport)) `
            -or ((Test-Path $preflightStderr) -and ((Get-Item $preflightStderr).Length -gt 0)) `
            -or ($null -ne $preflightExit -and $preflightExit -ne 0)
        if ($preflightFailed) {
            throw "Preflight failed (concurrentSuiteExit=$preflightTestExit, workloadExit=$preflightExit); inspect suite, workload and raw metric logs."
        }
        $status = 'PRECHECK_PASSED'
        Save-Summary $status
        return
    }
    $startedAt = Get-Date
    $deadline = $startedAt.AddHours($DurationHours)
    $status = 'RUNNING'
    'cycle,startedAtLocal,finishedAtLocal,elapsedSeconds,exitCode,status,log' | Set-Content -Encoding ascii -LiteralPath $resultsPath
    Save-Summary 'RUNNING'

    $cycle = 0; $failureSeen = $false
    while ((Get-Date) -lt $deadline) {
        $cycle++
        $cycleStarted = Get-Date
        $cycleLog = Join-Path $runDir ('cycle-{0:D4}-mvn-clean-test.log' -f $cycle)
        $cycleCodeFile = Join-Path $runDir ('cycle-{0:D4}-exit-code.txt' -f $cycle)
        $mavenProcess = Start-Process -FilePath $powershell -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$cycleWrapper,
            '-ProjectPath',$project,'-MavenPath',$maven,'-LogPath',$cycleLog,'-ExitCodePath',$cycleCodeFile) -WorkingDirectory $project `
            -RedirectStandardOutput (Join-Path $runDir ('cycle-{0:D4}-runner.stdout.log' -f $cycle)) `
            -RedirectStandardError (Join-Path $runDir ('cycle-{0:D4}-runner.stderr.log' -f $cycle)) `
            -WindowStyle Hidden -PassThru
        $cycleTimeout = $cycleStarted.AddMinutes(20)
        while (-not $mavenProcess.WaitForExit(10000)) {
            Add-ResourceSample
            if ($serverProcesses | Where-Object HasExited) { throw 'A soak application node exited unexpectedly; inspect node logs.' }
            if ($workloadProcess.HasExited) { throw 'Performance workload exited unexpectedly; inspect workload logs.' }
            if ((Get-Date) -gt $cycleTimeout) {
                & taskkill.exe /PID $mavenProcess.Id /T /F | Out-Null
                throw "Test cycle $cycle exceeded its 20-minute hang limit; process tree stopped and evidence retained."
            }
        }
        $mavenProcess.WaitForExit()
        $code = if (Test-Path $cycleCodeFile) { [int](Get-Content $cycleCodeFile -Raw).Trim() } else { 1 }
        if ($code -eq 0) {
            Push-Location $project
            try {
                & $python scripts/check-classfile-version.py target/classes target/test-classes *> (Join-Path $runDir ('cycle-{0:D4}-classfile-check.log' -f $cycle))
                if ($LASTEXITCODE -ne 0) { $code = $LASTEXITCODE }
            } finally { Pop-Location }
        }
        $cycleFinished = Get-Date
        $cycleStatus = if ($code -eq 0) { 'PASS' } else { 'FAIL' }
        [pscustomobject]@{cycle=$cycle;startedAtLocal=$cycleStarted.ToString('o');finishedAtLocal=$cycleFinished.ToString('o');
            elapsedSeconds=[math]::Round(($cycleFinished-$cycleStarted).TotalSeconds,3);exitCode=$code;status=$cycleStatus;log=(Split-Path $cycleLog -Leaf)} |
            Export-Csv -LiteralPath $resultsPath -NoTypeInformation -Append
        Write-Host "[$($cycleFinished.ToString('o'))] cycle=$cycle status=$cycleStatus exit=$code"
        if ($code -ne 0) { $failureSeen = $true; $status = 'FAILED'; Save-Summary $status; break }
        Add-ResourceSample
        Save-Summary 'RUNNING'
    }
    if ($failureSeen) { throw "Cycle $cycle failed; runner stopped to preserve its log and prevent a false PASS." }
    if ((Get-Date) -ge $deadline) { $status = 'PASS'; Save-Summary $status }
} catch {
    if ($status -eq 'PRECHECK') { $status = 'PRECHECK_FAILED' }
    elseif ($status -eq 'RUNNING') { $status = 'INTERRUPTED' }
    Save-Summary $status
    $_ | Out-String | Set-Content -Encoding utf8 -LiteralPath (Join-Path $runDir 'runner-error.txt')
    throw
} finally {
    if ($mavenProcess -and -not $mavenProcess.HasExited) {
        & taskkill.exe /PID $mavenProcess.Id /T /F | Out-Null
    }
    if ($workloadStarted) {
        New-Item -ItemType File -Force -Path $stopFile | Out-Null
        if ($workloadProcess -and -not $workloadProcess.HasExited) { $workloadProcess.WaitForExit(15000) | Out-Null }
        if ($workloadProcess -and -not $workloadProcess.HasExited) {
            & taskkill.exe /PID $workloadProcess.Id /T /F | Out-Null
        }
        if (-not (Test-Path (Join-Path $runDir 'workload\performance-summary.json')) -and $status -eq 'PASS') {
            $status = 'INCOMPLETE'
            Save-Summary $status
        }
    }
    foreach ($process in $serverProcesses) {
        if ($process -and -not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    }
    if ((-not $PreflightOnly) -and (Test-Path (Join-Path $runDir 'resource-samples.csv')) -and
            (Test-Path (Join-Path $runDir 'workload\performance-summary.json')) -and
            $workloadProcess -and $workloadProcess.HasExited) {
        & $python $metricsSummarizer --run-dir $runDir --processor-count ([Environment]::ProcessorCount) `
            *> (Join-Path $runDir 'metrics-summary.log')
        if ($LASTEXITCODE -ne 0) {
            'Resource/performance summary generation failed.' | Set-Content -Encoding utf8 -LiteralPath (Join-Path $runDir 'metrics-summary-error.txt')
            if ($status -eq 'PASS') { $status = 'INCOMPLETE' }
        }
    }
    foreach ($name in $previousEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $previousEnv[$name], 'Process') }
    if ($status -eq 'RUNNING') { $status = 'INCOMPLETE'; Save-Summary $status }
    Write-Host "Run status: $status; evidence: $runDir"
}
