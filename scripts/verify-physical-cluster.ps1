[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidateCount(3,32)][string[]]$Nodes,
    [int[]]$Ports = @(6379,7000,8080),
    [int[]]$PortsExpectedClosedFromThisClient = @(),
    [string]$HealthPath = '/api/health',
    [ValidateRange(1,65535)][int]$HttpsPort = 8443,
    [switch]$CheckHttpsHealth,
    [string]$ClientTokenEnvironmentVariable = 'KV_CLIENT_TOKEN',
    [string]$OutputPath = (Join-Path $PSScriptRoot '..\artifacts\physical-cluster-preflight.json')
)

$ErrorActionPreference = 'Stop'
$probeRows = [System.Collections.Generic.List[object]]::new()
$failures = 0
$parsed = [System.Collections.Generic.List[object]]::new()
foreach ($entry in $Nodes) {
    $parts = $entry -split '=',2
    if ($parts.Count -ne 2 -or [string]::IsNullOrWhiteSpace($parts[0]) -or [string]::IsNullOrWhiteSpace($parts[1])) {
        throw "Invalid node '$entry'. Use nodeId=hostname-or-IP."
    }
    $parsed.Add([pscustomobject]@{NodeId=$parts[0].Trim(); Host=$parts[1].Trim()})
}
if (@($parsed.NodeId | Select-Object -Unique).Count -ne $parsed.Count) { throw 'Node IDs must be unique.' }
if (@($parsed.Host | Select-Object -Unique).Count -ne $parsed.Count) { throw 'Each node must use a distinct routable host address; loopback aliases do not count as independent hosts.' }

$started = Get-Date
foreach ($node in $parsed) {
    foreach ($port in $Ports) {
        try {
            $probe = Test-NetConnection -ComputerName $node.Host -Port $port -WarningAction SilentlyContinue
            $status = if ($probe.TcpTestSucceeded) {'PASS'} else {'FAIL'}
            if ($status -eq 'FAIL') { $failures++ }
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$port; Check='TCP reachability'; Status=$status; Detail=$(if ($probe.TcpTestSucceeded) {'reachable'} else {'not reachable'}); Timestamp=(Get-Date).ToString('o')})
        } catch {
            $failures++
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$port; Check='TCP reachability'; Status='FAIL'; Detail=$_.Exception.Message; Timestamp=(Get-Date).ToString('o')})
        }
    }
    if ($CheckHttpsHealth) {
        try {
            $url = 'https://{0}:{1}{2}' -f $node.Host,$HttpsPort,$HealthPath
            $headers = @{}
            $token = [Environment]::GetEnvironmentVariable($ClientTokenEnvironmentVariable)
            if ($token) { $headers.Authorization = "Bearer $token" }
            $response = Invoke-WebRequest -Uri $url -Method Get -Headers $headers -UseBasicParsing -TimeoutSec 10
            $status = if ([int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 300) {'PASS'} else {'FAIL'}
            if ($status -eq 'FAIL') { $failures++ }
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$HttpsPort; Check='HTTPS health'; Status=$status; Detail="HTTP $([int]$response.StatusCode)"; Timestamp=(Get-Date).ToString('o')})
        } catch {
            $failures++
            $code = ''; try { $code = [int]$_.Exception.Response.StatusCode } catch {}
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$HttpsPort; Check='HTTPS health'; Status='FAIL'; Detail=$(if ($code) {"HTTP $code"} else {$_.Exception.Message}); Timestamp=(Get-Date).ToString('o')})
        }
    }
}
foreach ($node in $parsed) {
    foreach ($port in $PortsExpectedClosedFromThisClient) {
        try {
            $probe = Test-NetConnection -ComputerName $node.Host -Port $port -WarningAction SilentlyContinue
            $status = if ($probe.TcpTestSucceeded) {'FAIL'} else {'PASS'}
            if ($status -eq 'FAIL') { $failures++ }
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$port; Check='Must be closed from this client'; Status=$status; Detail=$(if ($probe.TcpTestSucceeded) {'reachable: review firewall scope'} else {'not reachable'}); Timestamp=(Get-Date).ToString('o')})
        } catch {
            $probeRows.Add([pscustomobject]@{NodeId=$node.NodeId; Host=$node.Host; Port=$port; Check='Must be closed from this client'; Status='PASS'; Detail='connection probe failed/closed'; Timestamp=(Get-Date).ToString('o')})
        }
    }
}

$report = [ordered]@{
    startedAt=$started.ToString('o'); finishedAt=(Get-Date).ToString('o'); initiatingComputer=$env:COMPUTERNAME
    nodeCount=$parsed.Count; nodes=@($parsed); portsChecked=@($Ports); closedFromClientPorts=@($PortsExpectedClosedFromThisClient)
    checks=@($probeRows); passCount=@($probeRows | Where-Object Status -eq 'PASS').Count
    failCount=@($probeRows | Where-Object Status -eq 'FAIL').Count
    conclusion=$(if ($failures -eq 0) {'PREFLIGHT_PASS_ONLY'} else {'PREFLIGHT_FAILED'})
    limitation='Reachability proves only that endpoints responded from this vantage point. It does not prove the nodes are physical machines, a shared Raft cluster, or that failover/migration is correct. Collect node IDs from server logs and run the failure scenarios in PHYSICAL_CLUSTER_VALIDATION.md.'
}
$outDir = Split-Path -Parent $OutputPath
if ($outDir) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $OutputPath
$probeRows | Export-Csv -NoTypeInformation -Encoding UTF8 ([IO.Path]::ChangeExtension($OutputPath,'.csv'))
Write-Host "Cluster preflight report: $OutputPath"
if ($failures -gt 0) { exit 1 }
