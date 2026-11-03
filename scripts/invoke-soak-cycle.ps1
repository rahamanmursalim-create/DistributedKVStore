param(
    [Parameter(Mandatory=$true)][string]$ProjectPath,
    [Parameter(Mandatory=$true)][string]$MavenPath,
    [Parameter(Mandatory=$true)][string]$LogPath,
    [Parameter(Mandatory=$true)][string]$ExitCodePath
)
$ErrorActionPreference = 'Continue'
Push-Location $ProjectPath
try {
    & $MavenPath -B clean test *> $LogPath
    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) { $exitCode = 1 }
} catch {
    $_ | Out-String | Add-Content -LiteralPath $LogPath
    $exitCode = 1
} finally {
    Pop-Location
}
[string]$exitCode | Set-Content -Encoding ascii -LiteralPath $ExitCodePath
exit $exitCode
