# Shared helpers for validation scripts. Compatible with Windows PowerShell 5.1+.
Set-StrictMode -Version Latest

function Get-MavenLauncher {
    $candidate = Get-Command mvn.cmd -ErrorAction SilentlyContinue
    if ($candidate) { return $candidate.Source }
    $candidate = Get-Command mvn -ErrorAction SilentlyContinue
    if ($candidate) { return $candidate.Source }
    throw 'Maven was not found. Open a shell with the project JDK and Maven configured.'
}

function Assert-NoConcurrentBuildWriter {
    try {
        $processes = @(Get-CimInstance Win32_Process -ErrorAction Stop)
        $matches = @($processes | Where-Object {
            $_.CommandLine -match 'org\.codehaus\.plexus\.classworlds\.launcher\.Launcher|org\.apache\.maven\.cli\.MavenCli'
        })
        if ($matches.Count -gt 0) {
            $details = ($matches | ForEach-Object { "PID=$($_.ProcessId) $($_.CommandLine)" }) -join [Environment]::NewLine
            throw "Another Maven process appears to be running. Stop it before starting validation to avoid overlapping builds.`n$details"
        }
        $foreignBuildWriters = @($processes | Where-Object {
            $_.Name -match '^javac\.exe$' -or
            $_.CommandLine -match 'com\.sun\.tools\.javac\.Main|org\.gradle\.launcher|GradleDaemon|org\.eclipse\.equinox\.launcher|org\.eclipse\.jdt\.ls\.core|org\.eclipse\.jdt\.internal\.compiler'
        })
        if ($foreignBuildWriters.Count -gt 0) {
            $details = ($foreignBuildWriters | ForEach-Object { "PID=$($_.ProcessId) $($_.Name) $($_.CommandLine)" }) -join [Environment]::NewLine
            throw "A non-Maven Java/build compiler is active and may write compiler output concurrently. Close it or run in an isolated checkout before Maven validation.`n$details"
        }
    } catch {
        if ($_.Exception.Message -like 'Another Maven process appears*' -or
            $_.Exception.Message -like 'A non-Maven Java/build compiler is active*') { throw }
        throw "Cannot verify that the build/compiler process list is clear. Validation is stopped to avoid corrupting target/classes. Run from an account that can inspect process command lines or use an isolated checkout and retry. Details: $($_.Exception.Message)"
    }
}

function Assert-Java17ClassFiles {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string[]]$Directories)

    $files = @()
    foreach ($directory in $Directories) {
        if (-not (Test-Path -LiteralPath $directory -PathType Container)) {
            throw "Expected compiled class directory is missing: $directory"
        }
        $files += @(Get-ChildItem -LiteralPath $directory -Filter '*.class' -File -Recurse)
    }
    if ($files.Count -eq 0) { throw 'No generated Java class files were found to verify.' }
    foreach ($file in $files) {
        $stream = [System.IO.File]::OpenRead($file.FullName)
        try {
            $header = New-Object byte[] 8
            if ($stream.Read($header, 0, $header.Length) -ne $header.Length)
                { throw "Truncated Java class file: $($file.FullName)" }
        } finally { $stream.Dispose() }
        if ($header[0] -ne 0xCA -or $header[1] -ne 0xFE -or $header[2] -ne 0xBA -or $header[3] -ne 0xBE)
            { throw "Invalid Java class magic: $($file.FullName)" }
        $major = ([int]$header[6] * 256) + [int]$header[7]
        if ($major -ne 61) { throw "Expected Java 17 class version 61, found $major in $($file.FullName)" }
    }
    Write-Host "Verified Java 17 class-file version 61 in $($files.Count) generated classes."
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
