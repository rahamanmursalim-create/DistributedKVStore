[CmdletBinding()]
param(
    [ValidateRange(1, 168)][int]$DurationHours = 1,
    [ValidateRange(1, 120)][int]$TestTimeoutMinutes = 10,
    [ValidateRange(0, 1000)][int]$FullSuiteEveryCycles = 0,
    # Optional bounded run for validating the harness itself. Each cycle still
    # executes the complete Maven custom suite; zero means run to duration.
    [ValidateRange(0, 10000)][int]$MaxCycles = 0,
    # Resolve script-dependent defaults after the param block: automatic
    # variables such as $PSScriptRoot are not reliable in parameter defaults.
    [string]$RepoRoot = '',
    [string]$ArtifactsRoot = '',
    [switch]$RequireEveryTestPresent,
    [switch]$SkipFinalFullSuite
)

$ErrorActionPreference = 'Stop'
$scriptRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    # Fallback for invocation environments where automatic script variables
    # are unavailable; normal -File execution should set $PSScriptRoot.
    $invokedPath = $MyInvocation.MyCommand.Path
    if (-not [string]::IsNullOrWhiteSpace($invokedPath)) {
        $scriptRoot = Split-Path -Parent $invokedPath
    }
}
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    throw 'Cannot determine the directory containing verify-long-soak.ps1.'
}
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $scriptRoot
}
if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Join-Path $RepoRoot 'artifacts'
}
. (Join-Path $scriptRoot 'validation-common.ps1')
$RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path
$ArtifactsRoot = [System.IO.Path]::GetFullPath($ArtifactsRoot)
$runId = 'raft-soak-' + (Get-Date -Format 'yyyyMMdd-HHmmss')
$outDir = Join-Path $ArtifactsRoot $runId
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$results = [System.Collections.Generic.List[object]]::new()
$started = Get-Date
$timer = [System.Diagnostics.Stopwatch]::StartNew()
$soakTimer = $null
$deadline = $null
$cycle = 0
$failed = $false
$missing = [System.Collections.Generic.List[string]]::new()

# Expected main-method integration tests in the current project. The Maven
# test phase runs the complete custom suite through AllTestsRunner.
$testClasses = @(
    'com.codecrafters.redis.raft.RaftMultiProcessTest',
    'com.codecrafters.redis.raft.RaftGroupConsensusTest',
    'com.codecrafters.redis.raft.RaftDynamicMembershipTest',
    'com.codecrafters.redis.raft.MultiRaftStoreTest',
    'com.codecrafters.redis.raft.RaftRebalanceIntegrationTest',
    'com.codecrafters.redis.raft.RaftSnapshotInstallTest',
    'com.codecrafters.redis.raft.RaftSlowDiskFaultTest',
    'com.codecrafters.redis.raft.RaftLinearizabilityTest',
    'com.codecrafters.redis.rpc.TlsTransportTest',
    'com.codecrafters.redis.web.WebServerIntegrationTest'
)

function Invoke-CapturedShellCommand {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string]$CommandText,
        [Parameter(Mandatory=$true)][string]$WorkingDirectory,
        [ValidateRange(1,60)][int]$TimeoutSeconds = 10
    )

    # Native tools such as `java -version` write to stderr. Calling them
    # directly through PowerShell with $ErrorActionPreference='Stop' can turn
    # that normal version output into a terminating NativeCommandError. Capture
    # output through cmd.exe instead, without changing the user's execution policy.
    $comspec = $env:ComSpec
    if ([string]::IsNullOrWhiteSpace($comspec)) {
        $comspec = Join-Path $env:SystemRoot 'System32\cmd.exe'
    }
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $comspec
    $startInfo.Arguments = '/d /s /c "' + $CommandText + ' 2>&1"'
    $startInfo.WorkingDirectory = (Resolve-Path $WorkingDirectory).Path
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    if (-not $process.Start()) { throw "Could not start command: $CommandText" }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $completed = $process.WaitForExit($TimeoutSeconds * 1000)
    if (-not $completed) {
        try { & taskkill.exe /PID $process.Id /T /F | Out-Null } catch { try { $process.Kill() } catch {} }
        try { $process.WaitForExit(5000) | Out-Null } catch {}
        throw "Command timed out after $TimeoutSeconds seconds: $CommandText"
    }
    $stdout = ''
    $stderr = ''
    try { $stdout = $stdoutTask.GetAwaiter().GetResult() } catch {}
    try { $stderr = $stderrTask.GetAwaiter().GetResult() } catch {}
    [pscustomobject]@{
        ExitCode = $process.ExitCode
        Output = ($stdout + [Environment]::NewLine + $stderr).Trim()
    }
}

function Assert-CustomMavenTestRunner {
    $pomPath = Join-Path $RepoRoot 'pom.xml'
    [xml]$pom = Get-Content -LiteralPath $pomPath -Raw
    $binding = $pom.SelectSingleNode("/*[local-name()='project']/*[local-name()='build']/*[local-name()='plugins']/*[local-name()='plugin'][*[local-name()='groupId']='org.codehaus.mojo' and *[local-name()='artifactId']='exec-maven-plugin']/*[local-name()='executions']/*[local-name()='execution'][*[local-name()='phase']='test']")
    if (-not $binding) { throw 'pom.xml must bind the custom test runner to Maven phase test.' }
    $goal = $binding.SelectSingleNode("./*[local-name()='goals']/*[local-name()='goal']")
    $mainClass = $binding.SelectSingleNode("./*[local-name()='configuration']/*[local-name()='mainClass']")
    $scope = $binding.SelectSingleNode("./*[local-name()='configuration']/*[local-name()='classpathScope']")
    if (-not $goal -or $goal.InnerText -ne 'java' -or
        -not $mainClass -or $mainClass.InnerText -ne 'com.codecrafters.redis.AllTestsRunner' -or
        -not $scope -or $scope.InnerText -ne 'test') {
        throw 'Maven test phase must run com.codecrafters.redis.AllTestsRunner with test classpath scope.'
    }
}

function Save-SoakSummary {
    $timer.Stop()
    if ($soakTimer) { $soakTimer.Stop() }
    $summary = [ordered]@{
        runId = $runId
        repoRoot = $RepoRoot
        startedAt = $started.ToString('o')
        finishedAt = (Get-Date).ToString('o')
        requestedDurationHours = $DurationHours
        maxCycles = $MaxCycles
        soakDurationSatisfied = ($soakTimer -and $soakTimer.Elapsed -ge [TimeSpan]::FromHours($DurationHours))
        mavenTestRunner = 'mvn clean test -> com.codecrafters.redis.AllTestsRunner (test classpath)'
        elapsedTotalSeconds = [Math]::Round($timer.Elapsed.TotalSeconds, 2)
        soakElapsedSeconds = $(if ($soakTimer) { [Math]::Round($soakTimer.Elapsed.TotalSeconds, 2) } else { 0 })
        cyclesStarted = $cycle
        allExecutedChecksPassed = (-not $failed)
        allExpectedTestClassesPresent = ($missing.Count -eq 0)
        missingTestClasses = @($missing)
        results = @($results)
        note = 'This is a same-host process soak. It is not physical-machine or availability-SLO evidence.'
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 (Join-Path $outDir 'summary.json')
    @($results) | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $outDir 'results.csv')
}

try {
    Assert-NoConcurrentMaven
    Assert-CustomMavenTestRunner
    $javaVersion = Invoke-CapturedShellCommand -CommandText 'java -version' -WorkingDirectory $RepoRoot -TimeoutSeconds 10
    if ($javaVersion.ExitCode -ne 0) { throw "java -version failed with exit code $($javaVersion.ExitCode): $($javaVersion.Output)" }
    $javaLines = @($javaVersion.Output -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $javaLine = [string]$javaLines[0]

    $mvnVersionResult = Invoke-CapturedShellCommand -CommandText 'mvn -version' -WorkingDirectory $RepoRoot -TimeoutSeconds 15
    if ($mvnVersionResult.ExitCode -ne 0) { throw "mvn -version failed with exit code $($mvnVersionResult.ExitCode): $($mvnVersionResult.Output)" }
    $mvnLines = @($mvnVersionResult.Output -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $mvnVersion = ($mvnLines | Select-Object -First 4) -join ' | '
    Write-Host "Java: $javaLine"
    Write-Host "Maven: $mvnVersion"
    if ($javaLine -notmatch '"17(?:\.|"|-)') {
        throw "This run is intended for Java 17, but 'java -version' reported: $javaLine. Set JAVA_HOME/PATH to Temurin 17 first."
    }
    if ($mvnVersion -notmatch 'Java version:\s*17(?:\.|$)') {
        throw "Maven does not appear to run on Java 17: $mvnVersion"
    }

    $compile = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments @('-q','-Daether.syncContext.named.factory=rwlock-local','clean','test-compile') -LogPath (Join-Path $outDir '00-test-compile.log') -TimeoutSeconds 900
    $results.Add([pscustomobject]@{ Cycle=0; TestClass='test-compile'; ExitCode=$compile.ExitCode; TimedOut=$compile.TimedOut; ElapsedSeconds=$compile.ElapsedSeconds; LogPath=$compile.LogPath; Status=$(if ($compile.ExitCode -eq 0) {'PASS'} else {'FAIL'}) })
    if ($compile.ExitCode -ne 0) { throw "test-compile failed; see $($compile.LogPath)" }

    # Also pick up newly added partition/fault/snapshot tests by name, without
    # depending on one specific class name from a previous code revision.
    $dynamicTests = @(Get-ChildItem (Join-Path $RepoRoot 'target/test-classes/com/codecrafters/redis') -Filter '*Test.class' -Recurse -ErrorAction SilentlyContinue | Where-Object {
        $_.Name -notmatch '\$' -and $_.Name -match '(Raft.*(Partition|Fault|Snapshot|SlowDisk|Linearizability|DynamicMembership|Rebalance|MultiProcess|Consensus).*Test|TlsTransportTest|WebServerIntegrationTest)\.class$'
    } | ForEach-Object {
        $relative = $_.FullName.Substring((Join-Path $RepoRoot 'target/test-classes').Length + 1)
        ($relative -replace '\\','.') -replace '\.class$',''
    })
    $testClasses = @($testClasses + $dynamicTests | Select-Object -Unique)
    $available = [System.Collections.Generic.List[string]]::new()
    foreach ($class in $testClasses) {
        $classFile = Join-Path $RepoRoot ('target/test-classes/' + ($class.Replace('.', '/') + '.class'))
        if (Test-Path $classFile) { $available.Add($class) } else { $missing.Add($class) }
    }
    $missing | Set-Content -Encoding UTF8 (Join-Path $outDir 'missing-test-classes.txt')
    if ($missing.Count -gt 0) {
        Write-Warning ("These requested test classes were not present in this checkout: " + ($missing -join ', '))
        if ($RequireEveryTestPresent) { throw 'Required long-soak test coverage is missing.' }
    }
    if ($available.Count -eq 0) { throw 'No integration test main classes were available to execute.' }

    $soakStarted = Get-Date
    $soakTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $deadline = [DateTimeOffset]::Now.AddHours($DurationHours)
    while ([DateTimeOffset]::Now -lt $deadline -and ($MaxCycles -eq 0 -or $cycle -lt $MaxCycles)) {
        $cycle++
        Write-Host "Soak cycle $cycle; elapsed $([Math]::Round($soakTimer.Elapsed.TotalMinutes,1)) minutes / target $($DurationHours * 60) minutes"
        if ([DateTimeOffset]::Now -lt $deadline) {
            $logPath = Join-Path $outDir ('cycle-{0:D4}-custom-test-suite.log' -f $cycle)
            # Always invoke the project lifecycle. Calling exec:java with a
            # test mainClass directly does not provide the forked process with
            # target/test-classes and breaks RaftMultiProcessTest's child JVM.
            # Clean every cycle so stale output from a different JDK/build can
            # never be reused, and fail before mutating target/ if another
            # Maven process appeared after the initial preflight.
            Assert-NoConcurrentMaven
            $suiteArgs = @('-q','-Daether.syncContext.named.factory=rwlock-local','clean','test')
            $suite = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments $suiteArgs -LogPath $logPath -TimeoutSeconds ($TestTimeoutMinutes * 60)
            $status = if ($suite.ExitCode -eq 0 -and -not $suite.TimedOut) {'PASS'} else {'FAIL'}
            $results.Add([pscustomobject]@{ Cycle=$cycle; TestClass='mvn clean test (AllTestsRunner custom suite)'; ExitCode=$suite.ExitCode; TimedOut=$suite.TimedOut; ElapsedSeconds=$suite.ElapsedSeconds; LogPath=$suite.LogPath; Status=$status })
            if ($status -eq 'FAIL') {
                $failed = $true
                Write-Warning "Failure in the custom Maven test suite; continuing to collect evidence. Log: $logPath"
            }
        }
        if ($FullSuiteEveryCycles -gt 0 -and ($cycle % $FullSuiteEveryCycles) -eq 0 -and [DateTimeOffset]::Now -lt $deadline) {
            $logPath = Join-Path $outDir ('cycle-{0:D4}-full-suite.log' -f $cycle)
            Assert-NoConcurrentMaven
            $full = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments @('-q','-Daether.syncContext.named.factory=rwlock-local','clean','test') -LogPath $logPath -TimeoutSeconds ($TestTimeoutMinutes * 60)
            $status = if ($full.ExitCode -eq 0 -and -not $full.TimedOut) {'PASS'} else {'FAIL'}
            $results.Add([pscustomobject]@{ Cycle=$cycle; TestClass='mvn clean test'; ExitCode=$full.ExitCode; TimedOut=$full.TimedOut; ElapsedSeconds=$full.ElapsedSeconds; LogPath=$full.LogPath; Status=$status })
            if ($status -eq 'FAIL') { $failed = $true }
        }
    }

    if (-not $SkipFinalFullSuite) {
        Write-Host 'Soak loop ended. Running one sequential clean full suite.'
        $logPath = Join-Path $outDir 'final-clean-test.log'
        Assert-NoConcurrentMaven
        $full = Invoke-BoundedMaven -RepoRoot $RepoRoot -MavenArguments @('-q','-Daether.syncContext.named.factory=rwlock-local','clean','test') -LogPath $logPath -TimeoutSeconds ($TestTimeoutMinutes * 60)
        $status = if ($full.ExitCode -eq 0 -and -not $full.TimedOut) {'PASS'} else {'FAIL'}
        $results.Add([pscustomobject]@{ Cycle=$cycle; TestClass='final mvn clean test'; ExitCode=$full.ExitCode; TimedOut=$full.TimedOut; ElapsedSeconds=$full.ElapsedSeconds; LogPath=$full.LogPath; Status=$status })
        if ($status -eq 'FAIL') { $failed = $true }
    }
} catch {
    $failed = $true
    $results.Add([pscustomobject]@{ Cycle=$cycle; TestClass='harness'; ExitCode=1; TimedOut=$false; ElapsedSeconds=0; LogPath=''; Status=('FAIL: ' + $_.Exception.Message) })
    Write-Error $_
} finally {
    Save-SoakSummary
    Write-Host "Evidence: $outDir"
}

if ($failed) { exit 1 }
