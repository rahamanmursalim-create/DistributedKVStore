# Shared helpers for validation scripts. Compatible with Windows PowerShell 5.1+.
Set-StrictMode -Version Latest

function Get-MavenLauncher {
    $candidate = Get-Command mvn.cmd -ErrorAction SilentlyContinue
    if ($candidate) { return $candidate.Source }
    $candidate = Get-Command mvn -ErrorAction SilentlyContinue
    if ($candidate) { return $candidate.Source }
    throw 'Maven was not found. Open a shell with the project JDK and Maven configured.'
}

function Assert-NoConcurrentMaven {
    try {
        $matches = @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
            $_.CommandLine -match 'org\.codehaus\.plexus\.classworlds\.launcher\.Launcher|org\.apache\.maven\.cli\.MavenCli'
        })
        if ($matches.Count -gt 0) {
            $details = ($matches | ForEach-Object { "PID=$($_.ProcessId) $($_.CommandLine)" }) -join [Environment]::NewLine
            throw "Another Maven process appears to be running. Stop it before starting validation to avoid overlapping builds.`n$details"
        }
    } catch {
        if ($_.Exception.Message -like 'Another Maven process appears*') { throw }
        Write-Warning "Could not inspect all running processes for Maven overlap: $($_.Exception.Message)"
    }
}

function Invoke-BoundedMaven {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string]$RepoRoot,
        [Parameter(Mandatory=$true)][string[]]$MavenArguments,
        [Parameter(Mandatory=$true)][string]$LogPath,
        [ValidateRange(1, 86400)][int]$TimeoutSeconds = 900
    )

    $launcher = Get-MavenLauncher
    $comspec = $env:ComSpec
    if ([string]::IsNullOrWhiteSpace($comspec)) { $comspec = Join-Path $env:SystemRoot 'System32\cmd.exe' }

    # Arguments used here contain no spaces; keeping the Maven launcher quoted is
    # sufficient for typical Windows installations under Program Files.
    $commandText = '"' + $launcher + '" ' + ($MavenArguments -join ' ')
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $comspec
    $startInfo.Arguments = '/d /s /c "' + $commandText + '"'
    $startInfo.WorkingDirectory = (Resolve-Path $RepoRoot).Path
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    $started = Get-Date
    if (-not $process.Start()) { throw "Could not start Maven command: $commandText" }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $completed = $process.WaitForExit($TimeoutSeconds * 1000)
    if (-not $completed) {
        try { & taskkill.exe /PID $process.Id /T /F | Out-Null } catch { try { $process.Kill() } catch {} }
        try { $process.WaitForExit(10000) | Out-Null } catch {}
    }
    $stdout = ''
    $stderr = ''
    try { $stdout = $stdoutTask.GetAwaiter().GetResult() } catch {}
    try { $stderr = $stderrTask.GetAwaiter().GetResult() } catch {}
    $exitCode = if ($completed) { $process.ExitCode } else { 124 }
    $elapsed = [Math]::Round(((Get-Date) - $started).TotalSeconds, 3)
    $content = @(
        "Started: $($started.ToString('o'))"
        "WorkingDirectory: $($startInfo.WorkingDirectory)"
        "Command: $commandText"
        "ElapsedSeconds: $elapsed"
        "TimedOut: $(-not $completed)"
        "ExitCode: $exitCode"
        '--- STDOUT ---'
        $stdout
        '--- STDERR ---'
        $stderr
    ) -join [Environment]::NewLine
    $parent = Split-Path -Parent $LogPath
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    [IO.File]::WriteAllText($LogPath, $content, [Text.UTF8Encoding]::new($false))

    [pscustomobject]@{
        Command = $commandText
        StartedAt = $started.ToString('o')
        ElapsedSeconds = $elapsed
        ExitCode = $exitCode
        TimedOut = (-not $completed)
        LogPath = $LogPath
        Output = ($stdout + [Environment]::NewLine + $stderr)
    }
}

function Get-Sha256Fingerprint {
    param([Parameter(Mandatory=$true)][byte[]]$Bytes)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash($Bytes)
        return (($hash | ForEach-Object { $_.ToString('X2') }) -join '')
    } finally { $sha.Dispose() }
}

function Normalize-Fingerprint {
    param([string]$Fingerprint)
    if ([string]::IsNullOrWhiteSpace($Fingerprint)) { return '' }
    return (($Fingerprint -replace '[^0-9A-Fa-f]', '').ToUpperInvariant())
}
