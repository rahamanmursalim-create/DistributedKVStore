param(
    [Parameter(Mandatory=$true)][string]$PythonPath,
    [Parameter(Mandatory=$true)][string]$WorkloadPath,
    [Parameter(Mandatory=$true)][string]$Port,
    [Parameter(Mandatory=$true)][string]$Concurrency,
    [Parameter(Mandatory=$true)][string]$TargetOpsPerSecond,
    [Parameter(Mandatory=$true)][string]$OutputPath,
    [Parameter(Mandatory=$true)][string]$StopFile,
    [Parameter(Mandatory=$true)][string]$CyclesFile,
    [Parameter(Mandatory=$true)][string]$ExitCodePath
)
$ErrorActionPreference = 'Continue'
& $PythonPath $WorkloadPath --port $Port --concurrency $Concurrency --target-ops-per-second $TargetOpsPerSecond --output $OutputPath --stop-file $StopFile --cycles-file $CyclesFile
$exitCode = $LASTEXITCODE
if ($null -eq $exitCode) { $exitCode = 1 }
[string]$exitCode | Set-Content -Encoding ascii -LiteralPath $ExitCodePath
exit $exitCode
