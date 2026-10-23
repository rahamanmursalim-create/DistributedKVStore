param(
    [Parameter(Mandatory = $true)][string[]]$NodeAddress,
    [Parameter(Mandatory = $true)][int[]]$RaftPort,
    [Parameter(Mandatory = $true)][string]$RemoteCommand
)
$ErrorActionPreference = 'Stop'
if ($NodeAddress.Count -lt 3 -or $RaftPort.Count -ne $NodeAddress.Count) {
    throw 'Supply at least three node addresses and one Raft port per node.'
}
if (($NodeAddress | Select-Object -Unique).Count -ne $NodeAddress.Count) {
    throw 'NodeAddress values must identify distinct hosts.'
}
Write-Host 'Physical-cluster preflight; this checks real endpoints and does not claim a cluster run.'
foreach ($address in $NodeAddress) {
    $resolved = [System.Net.Dns]::GetHostAddresses($address) | Where-Object { $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork }
    if (-not $resolved) { throw "No IPv4 address resolved for $address" }
    if ($address -in @('127.0.0.1', 'localhost') -or $resolved.IPAddressToString -contains '127.0.0.1') {
        throw "Loopback is not a physical host: $address"
    }
}
for ($i = 0; $i -lt $NodeAddress.Count; $i++) {
    foreach ($port in $RaftPort) {
        if ($NodeAddress[$i] -eq $NodeAddress[0] -and $port -eq $RaftPort[$i]) { continue }
        $result = Test-NetConnection -ComputerName $NodeAddress[$i] -Port $port -WarningAction SilentlyContinue
        if (-not $result.TcpTestSucceeded) { throw "TCP $($NodeAddress[$i]):$port is unreachable; inspect listener/firewall/routing." }
    }
}
if (-not (Get-Command ssh.exe -ErrorAction SilentlyContinue)) { throw 'OpenSSH client is required to run the remote verifier.' }
foreach ($address in $NodeAddress) {
    Write-Host "Checking remote Java 17+ and test command on $address"
    & ssh.exe -- $address $RemoteCommand
    if ($LASTEXITCODE -ne 0) { throw "Remote verification command failed on $address with exit code $LASTEXITCODE" }
}
Write-Host 'Preflight passed. Start one isolated node process per host with the same cluster ID, voter IDs, and peer addresses; then run failure/partition/recovery schedule from scripts/PHYSICAL_CLUSTER_TEST.md.'
