param(
    [string]$ArtifactDirectory = "artifacts/docker-cluster-$(Get-Date -Format yyyyMMdd-HHmmss)"
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$artifact = [System.IO.Path]::GetFullPath((Join-Path $root $ArtifactDirectory))
New-Item -ItemType Directory -Force -Path $artifact | Out-Null
$composeFile = Join-Path $artifact 'compose.yml'
$keystore = Join-Path $artifact 'identity.p12'
$rotatedKeystore = Join-Path $artifact 'identity-rotated.p12'
$caFile = Join-Path $artifact 'ca.pem'
$rotatedCaFile = Join-Path $artifact 'rotated-ca.pem'
$logFile = Join-Path $artifact 'docker.log'
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
$passwordBytes = New-Object byte[] 24; $clientTokenBytes = New-Object byte[] 32; $clusterTokenBytes = New-Object byte[] 32
$rng.GetBytes($passwordBytes); $rng.GetBytes($clientTokenBytes); $rng.GetBytes($clusterTokenBytes); $rng.Dispose()
$password = [Convert]::ToBase64String($passwordBytes).TrimEnd('=').Replace('+','A').Replace('/','B')
$clientToken = [Convert]::ToBase64String($clientTokenBytes).TrimEnd('=').Replace('+','C').Replace('/','D')
$clusterToken = [Convert]::ToBase64String($clusterTokenBytes).TrimEnd('=').Replace('+','E').Replace('/','F')
$javaPath = (Get-Command java.exe -ErrorAction Stop).Source
$previousPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$javaHomeLine = & $javaPath -XshowSettings:properties -version 2>&1 | Select-String 'java.home ='
$ErrorActionPreference = $previousPreference
$javaHome = $javaHomeLine.Line.Split('=', 2)[1].Trim()
$keytool = Join-Path $javaHome 'bin/keytool.exe'
if (-not (Test-Path -LiteralPath $keytool)) { throw "keytool not found under $javaHome" }
if (Test-Path -LiteralPath $keystore) { Remove-Item -LiteralPath $keystore -Force }
if (Test-Path -LiteralPath $rotatedKeystore) { Remove-Item -LiteralPath $rotatedKeystore -Force }
$keyProcess = Start-Process -FilePath $keytool -ArgumentList @('-genkeypair','-alias','docker-test','-keyalg','RSA','-keysize','2048','-validity','2','-dname','CN=localhost','-ext','SAN=dns:localhost,ip:127.0.0.1','-storetype','PKCS12','-keystore',$keystore,'-storepass',$password,'-keypass',$password,'-noprompt') -Wait -PassThru -NoNewWindow
if ($keyProcess.ExitCode -ne 0) { throw 'Could not generate temporary test identity.' }
$rotateProcess = Start-Process -FilePath $keytool -ArgumentList @('-genkeypair','-alias','docker-test-rotated','-keyalg','RSA','-keysize','2048','-validity','2','-dname','CN=localhost','-ext','SAN=dns:localhost,ip:127.0.0.1','-storetype','PKCS12','-keystore',$rotatedKeystore,'-storepass',$password,'-keypass',$password,'-noprompt') -Wait -PassThru -NoNewWindow
if ($rotateProcess.ExitCode -ne 0) { throw 'Could not generate rotated test identity.' }
$compose = @"
services:
  kv1:
    build: { context: "$($root -replace '\\','/')", dockerfile: Dockerfile.verify }
    image: distributed-kvstore:docker-check
    container_name: kvverify-n1
    pull_policy: never
    hostname: kv1
    restart: unless-stopped
    ports: ["18081:8080", "16379:6379"]
    volumes: ["./identity.p12:/run/kv/identity.p12:ro", "kv1data:/var/data"]
    environment: &cluster-env
      PORT: "8080"
      TCP_PORT: "6379"
      DATA_DIR: /var/data
      KV_BIND_HOST: 0.0.0.0
      KV_CLIENT_TOKEN: "$clientToken"
      KV_CLUSTER_TOKEN: "$clusterToken"
      KV_TLS_KEYSTORE: /run/kv/identity.p12
      KV_TLS_TRUSTSTORE: /run/kv/identity.p12
      KV_TLS_PASSWORD: "$password"
      KV_TLS_STORE_TYPE: PKCS12
      KV_NODE_ID: n1
      KV_RAFT_GROUP_ID: docker-check
      KV_RAFT_BIND_HOST: 0.0.0.0
      KV_RAFT_PORT: "7000"
      KV_RAFT_ELECTION_MIN_MS: "6000"
      KV_RAFT_ELECTION_MAX_MS: "9000"
      KV_RAFT_RPC_CONNECT_TIMEOUT_MS: "1500"
      KV_RAFT_RPC_READ_TIMEOUT_MS: "3000"
      KV_RAFT_PEERS: n1=172.29.99.11:7000,n2=172.29.99.12:7000,n3=172.29.99.13:7000
      JAVA_TOOL_OPTIONS: -Dkv.raft.debug=true
    networks:
      raftnet: { ipv4_address: 172.29.99.11 }
  kv2:
    image: distributed-kvstore:docker-check
    container_name: kvverify-n2
    pull_policy: never
    hostname: kv2
    restart: unless-stopped
    ports: ["18082:8080", "16380:6379"]
    volumes: ["./identity.p12:/run/kv/identity.p12:ro", "kv2data:/var/data"]
    environment:
      <<: *cluster-env
      KV_NODE_ID: n2
    networks:
      raftnet: { ipv4_address: 172.29.99.12 }
  kv3:
    image: distributed-kvstore:docker-check
    container_name: kvverify-n3
    pull_policy: never
    hostname: kv3
    restart: unless-stopped
    ports: ["18083:8080", "16381:6379"]
    volumes: ["./identity.p12:/run/kv/identity.p12:ro", "kv3data:/var/data"]
    environment:
      <<: *cluster-env
      KV_NODE_ID: n3
    networks:
      raftnet: { ipv4_address: 172.29.99.13 }
networks:
  raftnet:
    name: kvverify-raftnet
    ipam:
      config: [{ subnet: 172.29.99.0/24 }]
volumes:
  kv1data:
  kv2data:
  kv3data:
"@
Set-Content -LiteralPath $composeFile -Value $compose -Encoding utf8
$composeArgs = @('compose','-p','kvverify','-f',$composeFile)
$checks = [System.Collections.Generic.List[string]]::new()
$script:caFile = $caFile
function Request-Api([int]$nodePort, [string]$method, [string]$path, [string]$body = $null, [string]$token = $clientToken) {
    $arguments = @('--silent','--show-error','--max-time','20','--cacert',$script:caFile,'-X',$method,
        '-w',"`n%{http_code}")
    if ($token) { $arguments += @('-H',"Authorization: Bearer $token") }
    if (-not [string]::IsNullOrEmpty($body)) { $arguments += @('--data-binary',$body) }
    $arguments += "https://localhost:$nodePort$path"
    $raw = @(& curl.exe @arguments 2>&1 | ForEach-Object { $_.ToString() }) -join "`n"
    $curlExit = $LASTEXITCODE
    if ($curlExit -ne 0) { throw "HTTPS request $method $nodePort$path failed (curl=$curlExit): $raw" }
    $lastNewline = $raw.LastIndexOf("`n")
    if ($lastNewline -lt 0) { throw "HTTPS response omitted status code: $raw" }
    $status = 0
    if (-not [int]::TryParse($raw.Substring($lastNewline + 1).Trim(), [ref]$status)) { throw "Invalid HTTP status in response: $raw" }
    return [pscustomobject]@{ StatusCode = $status; Content = $raw.Substring(0,$lastNewline) }
}
function Wait-Leader([int[]]$ports, [string]$except = '') {
    $deadline = (Get-Date).AddSeconds(25)
    do {
        foreach ($port in $ports) {
            try {
                $response = Request-Api $port 'GET' '/api/raft/status'
                if ([int]$response.StatusCode -eq 200) {
                    $data = $response.Content | ConvertFrom-Json
                    foreach ($group in $data.groups.PSObject.Properties) {
                        if ($group.Value.localLeader -and $group.Value.leader -ne $except) { return $group.Value.leader }
                    }
                }
            } catch { }
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "No stable elected leader found among HTTP ports $($ports -join ',')"
}
function Put-Check([int]$port, [string]$key, [string]$value) {
    $deadline = (Get-Date).AddSeconds(30); $last = ''
    do {
        try {
            $response = Request-Api $port 'PUT' "/api/keys/$key" $value
            if ([int]$response.StatusCode -eq 200) { return }
            $last = "HTTP $([int]$response.StatusCode): $($response.Content)"
        } catch { $last = $_.Exception.Message }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "PUT $key did not commit before timeout: $last"
}
function Get-Check([int]$port, [string]$key, [string]$expected) {
    $deadline = (Get-Date).AddSeconds(20); $last = ''
    do {
        try {
            $response = Request-Api $port 'GET' "/api/keys/$key"
            if ([int]$response.StatusCode -eq 200 -and $response.Content -match [regex]::Escape($expected)) { return }
            $last = "HTTP $([int]$response.StatusCode): $($response.Content)"
        } catch { $last = $_.Exception.Message }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "GET $key on $port did not return committed value ${expected}: $last"
}
function Invoke-Docker([string[]]$Arguments, [string]$LogPath = $null) {
    $preference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = @(& docker @Arguments 2>&1 | ForEach-Object { $_.ToString() })
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $preference }
    if ($LogPath) { $output | Add-Content -LiteralPath $LogPath }
    else { $output | ForEach-Object { Write-Host $_ } }
    if ($code -ne 0) { throw "docker $($Arguments -join ' ') failed with exit code ${code}: $($output -join ' ')" }
    return $output
}
function Read-Thumbprint([string]$Store) {
    $certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($Store, $password)
    try { return $certificate.Thumbprint.ToUpperInvariant() } finally { $certificate.Dispose() }
}
function Get-ServerThumbprint([int]$port) {
    $script:observedThumbprint = $null
    $callback = [System.Net.Security.RemoteCertificateValidationCallback]{
        param($sender, $certificate, $chain, $errors)
        $script:observedThumbprint = ([System.Security.Cryptography.X509Certificates.X509Certificate2]::new($certificate)).Thumbprint.ToUpperInvariant()
        return $true
    }
    $tcp = [System.Net.Sockets.TcpClient]::new('localhost', $port)
    $ssl = $null
    try {
        $ssl = [System.Net.Security.SslStream]::new($tcp.GetStream(), $false, $callback)
        $ssl.AuthenticateAsClient('localhost')
        return $script:observedThumbprint
    } finally {
        if ($ssl) { $ssl.Dispose() }
        $tcp.Dispose()
    }
}

try {
    $docker = (Invoke-Docker @('info','--format','{{.ServerVersion}}')) -join ''
    & mvn -q "-DskipTests" "-Dexec.skip=true" package
    if ($LASTEXITCODE -ne 0) { throw 'Local Maven package build failed.' }
    $listing = & $keytool -list -v -keystore $keystore -storetype PKCS12 -storepass $password 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Could not validate generated keystore: $listing" }
    $script:expectedThumbprint = Read-Thumbprint $keystore
    $rotatedThumbprint = Read-Thumbprint $rotatedKeystore
    & $keytool -exportcert -rfc -alias docker-test -keystore $keystore -storetype PKCS12 -storepass $password -file $caFile | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not export test CA certificate.' }
    & $keytool -exportcert -rfc -alias docker-test-rotated -keystore $rotatedKeystore -storetype PKCS12 -storepass $password -file $rotatedCaFile | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not export rotated test CA certificate.' }
    $checks.Add("Docker Engine $docker")
    Invoke-Docker ($composeArgs + @('build','kv1')) $logFile | Out-Null
    Invoke-Docker ($composeArgs + @('up','-d','--no-build','--pull','never')) $logFile | Out-Null
    $nodePorts = @(18081,18082,18083)
    $deadline = (Get-Date).AddMinutes(3)
    $lastReadinessError = ''
    do {
        $ready = 0
        foreach ($port in $nodePorts) { try { $r = Request-Api $port 'GET' '/api/health'; if ([int]$r.StatusCode -eq 200) { $ready++ } else { $lastReadinessError = "port=$port http=$([int]$r.StatusCode)" } } catch { $lastReadinessError = "port=$port error=$($_.Exception.ToString())" } }
        if ($ready -eq 3) { break }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if ($ready -ne 3) { throw "Only $ready/3 HTTPS nodes became ready. lastError=$lastReadinessError expectedThumbprint=$script:expectedThumbprint" }
    $denied = Request-Api 18081 'GET' '/api/health' $null ''
    if ([int]$denied.StatusCode -ne 401) { throw 'Unauthenticated HTTPS client was not rejected.' }
    $checks.Add('three isolated containers ready; HTTPS authentication enforced')
    $leader = Wait-Leader $nodePorts
    $checks.Add("initial leader $leader")
    Put-Check 18081 'docker-seed' 'replicated'
    foreach ($port in $nodePorts) { Get-Check $port 'docker-seed' 'replicated' }
    $checks.Add('seed write replicated and read through all container HTTPS endpoints')

    # Stop a follower process; the live leader and remaining voter must retain quorum.
    $followerIndex = 1..3 | Where-Object { "n$_" -ne $leader } | Select-Object -First 1
    $failedPort = $nodePorts[$followerIndex - 1]
    $leaderPort = $nodePorts[[int]$leader.Substring(1) - 1]
    Invoke-Docker @('stop',"kvverify-n$followerIndex") | Out-Null
    Put-Check $leaderPort 'after-follower-failure' 'quorum-committed'
    $survivors = @($nodePorts | Where-Object { $_ -ne $failedPort })
    foreach ($port in $survivors) { Get-Check $port 'after-follower-failure' 'quorum-committed' }
    Invoke-Docker @('start',"kvverify-n$followerIndex") | Out-Null
    $null = Wait-Leader $nodePorts
    Get-Check $failedPort 'after-follower-failure' 'quorum-committed'
    $checks.Add("follower n$followerIndex stopped; majority committed, restarted follower caught up")

    # Keep the HTTP process alive but pause both peers: a singleton must fail closed without quorum.
    $leader = Wait-Leader $nodePorts
    $leaderPort = $nodePorts[[int]$leader.Substring(1) - 1]
    $followers = @(1..3 | Where-Object { "n$_" -ne $leader })
    foreach ($index in $followers) { Invoke-Docker @('pause',"kvverify-n$index") | Out-Null }
    $noQuorum = $false
    try { $response = Request-Api $leaderPort 'PUT' '/api/keys/no-quorum' 'must-fail'; $noQuorum = [int]$response.StatusCode -ne 200 }
    catch { $noQuorum = $true }
    foreach ($index in $followers) { Invoke-Docker @('unpause',"kvverify-n$index") | Out-Null }
    if (-not $noQuorum) { throw 'A Raft group acknowledged a write while only one voter was running.' }
    $checks.Add('single running voter could not commit without quorum; peers resumed')

    # Rotate the cluster identity on disk, restart every replica, and pin the new leaf certificate.
    $oldThumbprint = $script:expectedThumbprint
    Invoke-Docker ($composeArgs + @('stop')) | Out-Null
    Copy-Item -LiteralPath $rotatedKeystore -Destination $keystore -Force
    $script:expectedThumbprint = $rotatedThumbprint
    $script:caFile = $rotatedCaFile
    Invoke-Docker ($composeArgs + @('up','-d','--force-recreate','--no-build','--pull','never')) | Out-Null
    $null = Wait-Leader $nodePorts
    $rotatedHealth = $null
    $rotationDeadline = (Get-Date).AddSeconds(30)
    do {
        try { $rotatedHealth = Request-Api 18081 'GET' '/api/health'; if ([int]$rotatedHealth.StatusCode -eq 200) { break } }
        catch { $rotationError = $_.Exception.Message }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $rotationDeadline)
    if ($null -eq $rotatedHealth -or [int]$rotatedHealth.StatusCode -ne 200) { throw "Rotated TLS certificate did not serve authenticated HTTPS successfully: $rotationError" }
    $observedRotatedThumbprint = Get-ServerThumbprint 18081
    if ($observedRotatedThumbprint -ne $rotatedThumbprint) { throw "HTTPS listener served unexpected leaf certificate $observedRotatedThumbprint after rotation; expected $rotatedThumbprint" }
    $oldRejected = $observedRotatedThumbprint -ne $oldThumbprint
    if (-not $oldRejected) { throw 'HTTPS listener continued presenting the old leaf certificate after rotation.' }
    $script:expectedThumbprint = $rotatedThumbprint
    $script:caFile = $rotatedCaFile
    $checks.Add("cluster certificate rotated from $oldThumbprint to $rotatedThumbprint; old pin rejected after restart")

    Invoke-Docker ($composeArgs + @('logs','--no-color')) | Set-Content -LiteralPath (Join-Path $artifact 'node-logs.txt')
    [ordered]@{ result='PASS'; finishedAt=(Get-Date).ToString('o'); dockerEngine=$docker; certificateThumbprint=$script:expectedThumbprint; checks=$checks; dockerBridgePartitionTest='not-run; Docker Desktop bridge disconnect timed out in prior attempt'; physicalMachineTest=$false; note='Three distinct container network namespaces on one physical host.' } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $artifact 'summary.json')
    Write-Host "Docker cluster verification passed. Artifacts: $artifact"
} catch {
    [ordered]@{ result='FAIL'; finishedAt=(Get-Date).ToString('o'); error=$_.Exception.Message; checks=$checks; physicalMachineTest=$false } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $artifact 'summary.json')
    throw
} finally {
    try { Invoke-Docker ($composeArgs + @('logs','--no-color')) | Set-Content -LiteralPath (Join-Path $artifact 'node-logs.txt') } catch { }
    try { Invoke-Docker ($composeArgs + @('down','--volumes','--remove-orphans')) $logFile | Out-Null } catch { }
    Remove-Item -LiteralPath $composeFile,$keystore,$rotatedKeystore,$caFile,$rotatedCaFile -Force -ErrorAction SilentlyContinue
}
