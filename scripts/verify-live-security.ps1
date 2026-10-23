[CmdletBinding()]
param(
    [string]$HttpsHealthUrl,
    [string]$ExpectedHttpsSha256,
    [string]$PreviousHttpsSha256,
    [string]$RespTlsHost,
    [ValidateRange(1,65535)][int]$RespTlsPort = 0,
    [string]$ExpectedRespSha256,
    [string]$PreviousRespSha256,
    [string]$RaftTlsHost,
    [ValidateRange(1,65535)][int]$RaftTlsPort = 0,
    [string]$ExpectedRaftSha256,
    [string]$PreviousRaftSha256,
    [string]$ClientPfxPath,
    [string]$PfxPasswordEnvironmentVariable = 'KV_TEST_CLIENT_PFX_PASSWORD',
    [string]$PreviousClientPfxPath,
    [string]$PreviousClientPfxPasswordEnvironmentVariable = 'KV_PREVIOUS_TEST_CLIENT_PFX_PASSWORD',
    [string]$ClientTokenEnvironmentVariable = 'KV_CLIENT_TOKEN',
    [int[]]$PortsExpectedClosedFromClient = @(),
    [string]$ReportPath = (Join-Path $PSScriptRoot '..\artifacts\live-security-verification.json')
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'validation-common.ps1')
$checks = [System.Collections.Generic.List[object]]::new()
$failures = 0

function Add-Check {
    param([string]$Name, [string]$Status, [string]$Detail)
    $checks.Add([pscustomobject]@{ Name=$Name; Status=$Status; Detail=$Detail; Time=(Get-Date).ToString('o') })
    if ($Status -ne 'PASS') { $script:failures++ }
    Write-Host ("[{0}] {1}: {2}" -f $Status, $Name, $Detail)
}

function Get-TlsCertificateInfo {
    param(
        [Parameter(Mandatory=$true)][string]$HostName,
        [Parameter(Mandatory=$true)][int]$Port,
        [string]$ExpectedFingerprint,
        [string]$PreviousFingerprint,
        [string]$ClientCertificatePathOverride,
        [string]$ClientCertificatePasswordEnvironmentVariable,
        [switch]$UseClientCertificate
    )
    $tcp = [System.Net.Sockets.TcpClient]::new()
    $tcp.SendTimeout = 5000; $tcp.ReceiveTimeout = 5000
    $ssl = $null; $cert = $null
    try {
        $tcp.Connect($HostName, $Port)
        $ssl = [System.Net.Security.SslStream]::new($tcp.GetStream(), $false)
        if ($UseClientCertificate) {
            $selectedPfx = if ($ClientCertificatePathOverride) { $ClientCertificatePathOverride } else { $ClientPfxPath }
            $selectedPasswordVariable = if ($ClientCertificatePasswordEnvironmentVariable) { $ClientCertificatePasswordEnvironmentVariable } else { $PfxPasswordEnvironmentVariable }
            if ([string]::IsNullOrWhiteSpace($selectedPfx) -or -not (Test-Path $selectedPfx)) {
                throw 'A valid client PFX path is required for the Raft mTLS check.'
            }
            $pfxPassword = [Environment]::GetEnvironmentVariable($selectedPasswordVariable)
            $clientCert = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
                (Resolve-Path $selectedPfx).Path,
                $pfxPassword,
                [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet)
            $certs = New-Object System.Security.Cryptography.X509Certificates.X509CertificateCollection
            [void]$certs.Add($clientCert)
            $ssl.AuthenticateAsClient($HostName, $certs, [System.Security.Authentication.SslProtocols]::None, $true)
        } else {
            $ssl.AuthenticateAsClient($HostName)
        }
        $cert = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($ssl.RemoteCertificate)
        $fingerprint = Get-Sha256Fingerprint -Bytes $cert.GetRawCertData()
        if ($ExpectedFingerprint) {
            $wanted = Normalize-Fingerprint $ExpectedFingerprint
            if ($fingerprint -ne $wanted) { throw "Certificate fingerprint mismatch. Expected $wanted, got $fingerprint" }
        }
        if ($PreviousFingerprint) {
            $previous = Normalize-Fingerprint $PreviousFingerprint
            if ($fingerprint -eq $previous) { throw "Certificate rotation was expected, but endpoint still presents previous fingerprint $previous" }
        }
        return [pscustomobject]@{ Fingerprint=$fingerprint; Subject=$cert.Subject; Issuer=$cert.Issuer; NotBefore=$cert.NotBefore.ToString('o'); NotAfter=$cert.NotAfter.ToString('o'); Protocol=$ssl.SslProtocol.ToString() }
    } finally {
        if ($cert) { $cert.Dispose() }
        if ($ssl) { $ssl.Dispose() }
        $tcp.Dispose()
    }
}

function Get-HttpStatus {
    param([string]$Url, [hashtable]$Headers)
    try {
        $response = Invoke-WebRequest -Uri $Url -Method Get -Headers $Headers -UseBasicParsing -TimeoutSec 15
        return [int]$response.StatusCode
    } catch {
        if ($_.Exception.Response) {
            try { return [int]$_.Exception.Response.StatusCode } catch { }
            try { return [int]$_.Exception.Response.StatusCode.value__ } catch { }
        }
        throw
    }
}

function Send-RespLine {
    param([System.IO.StreamReader]$Reader, [System.IO.StreamWriter]$Writer, [string[]]$Parts)
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('*').Append($Parts.Count).Append("`r`n")
    foreach ($part in $Parts) {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($part)
        [void]$builder.Append('$').Append($bytes.Length).Append("`r`n").Append($part).Append("`r`n")
    }
    $Writer.Write($builder.ToString()); $Writer.Flush()
    return $Reader.ReadLine()
}

if (-not $HttpsHealthUrl -and -not ($RespTlsHost -and $RespTlsPort -gt 0) -and -not ($RaftTlsHost -and $RaftTlsPort -gt 0)) {
    throw 'Supply at least one HTTPS URL, RESP TLS endpoint, or Raft TLS endpoint.'
}

if ($HttpsHealthUrl) {
    try {
        $uri = [Uri]$HttpsHealthUrl
        if ($uri.Scheme -ne 'https') { throw 'The health URL must use https://; refusing to follow an HTTP-only URL.' }
        $info = Get-TlsCertificateInfo -HostName $uri.DnsSafeHost -Port $uri.Port -ExpectedFingerprint $ExpectedHttpsSha256 -PreviousFingerprint $PreviousHttpsSha256
        Add-Check 'HTTPS certificate chain/fingerprint' 'PASS' ("host={0}; fingerprint={1}; protocol={2}; expires={3}" -f $uri.DnsSafeHost,$info.Fingerprint,$info.Protocol,$info.NotAfter)

        $token = [Environment]::GetEnvironmentVariable($ClientTokenEnvironmentVariable)
        if ([string]::IsNullOrWhiteSpace($token)) { throw "Environment variable $ClientTokenEnvironmentVariable is empty; token value is intentionally not accepted on the command line." }
        $withoutTokenStatus = Get-HttpStatus -Url $HttpsHealthUrl -Headers @{}
        if ($withoutTokenStatus -in @(401,403)) { Add-Check 'HTTPS unauthenticated request' 'PASS' "rejected with HTTP $withoutTokenStatus" }
        else { Add-Check 'HTTPS unauthenticated request' 'FAIL' "expected 401/403, got HTTP $withoutTokenStatus" }
        $withTokenStatus = Get-HttpStatus -Url $HttpsHealthUrl -Headers @{ Authorization = "Bearer $token" }
        if ($withTokenStatus -ge 200 -and $withTokenStatus -lt 300) { Add-Check 'HTTPS authenticated request' 'PASS' "HTTP $withTokenStatus" }
        else { Add-Check 'HTTPS authenticated request' 'FAIL' "expected 2xx, got HTTP $withTokenStatus" }
        $badTokenStatus = Get-HttpStatus -Url $HttpsHealthUrl -Headers @{ Authorization = "Bearer $token.invalid-validation-token" }
        if ($badTokenStatus -in @(401,403)) { Add-Check 'HTTPS invalid token rejected' 'PASS' "HTTP $badTokenStatus" }
        else { Add-Check 'HTTPS invalid token rejected' 'FAIL' "expected 401/403, got HTTP $badTokenStatus" }
    } catch { Add-Check 'HTTPS deployment check' 'FAIL' $_.Exception.Message }
}

if ($RespTlsHost -and $RespTlsPort -gt 0) {
    $tcp = $null; $ssl = $null; $reader = $null; $writer = $null
    try {
        $info = Get-TlsCertificateInfo -HostName $RespTlsHost -Port $RespTlsPort -ExpectedFingerprint $ExpectedRespSha256 -PreviousFingerprint $PreviousRespSha256
        Add-Check 'RESP TLS certificate chain/fingerprint' 'PASS' ("host={0}; port={1}; fingerprint={2}; protocol={3}" -f $RespTlsHost,$RespTlsPort,$info.Fingerprint,$info.Protocol)
        $tcp = [System.Net.Sockets.TcpClient]::new()
        $tcp.Connect($RespTlsHost, $RespTlsPort)
        $ssl = [System.Net.Security.SslStream]::new($tcp.GetStream(), $false)
        $ssl.AuthenticateAsClient($RespTlsHost)
        $reader = [System.IO.StreamReader]::new($ssl, [System.Text.Encoding]::UTF8, $false, 1024, $true)
        $writer = [System.IO.StreamWriter]::new($ssl, [System.Text.UTF8Encoding]::new($false), 1024, $true)
        $token = [Environment]::GetEnvironmentVariable($ClientTokenEnvironmentVariable)
        if ([string]::IsNullOrWhiteSpace($token)) { throw "Environment variable $ClientTokenEnvironmentVariable is empty." }
        $authReply = Send-RespLine -Reader $reader -Writer $writer -Parts @('AUTH',$token)
        if ($authReply -notlike '+OK*') { throw "RESP AUTH did not return +OK (reply type: $($authReply.Substring(0,[Math]::Min(32,$authReply.Length))))" }
        $pingReply = Send-RespLine -Reader $reader -Writer $writer -Parts @('PING')
        if ($pingReply -ne '+PONG') { throw "RESP PING expected +PONG, got '$pingReply'" }
        Add-Check 'RESP TLS AUTH/PING' 'PASS' 'TLS handshake, AUTH, and PING passed; token not logged.'
    } catch { Add-Check 'RESP TLS endpoint' 'FAIL' $_.Exception.Message }
    finally {
        if ($reader) { $reader.Dispose() }; if ($writer) { $writer.Dispose() }; if ($ssl) { $ssl.Dispose() }; if ($tcp) { $tcp.Dispose() }
    }

    # Check a bad token on a fresh, unauthenticated TLS connection.
    $badTcp = $null; $badSsl = $null; $badReader = $null; $badWriter = $null; $badConnected = $false
    try {
        $badTcp = [System.Net.Sockets.TcpClient]::new(); $badTcp.Connect($RespTlsHost,$RespTlsPort); $badConnected = $true
        $badSsl = [System.Net.Security.SslStream]::new($badTcp.GetStream(),$false)
        $badSsl.AuthenticateAsClient($RespTlsHost)
        $badReader = [System.IO.StreamReader]::new($badSsl,[System.Text.Encoding]::UTF8,$false,1024,$true)
        $badWriter = [System.IO.StreamWriter]::new($badSsl,[System.Text.UTF8Encoding]::new($false),1024,$true)
        $badReply = Send-RespLine -Reader $badReader -Writer $badWriter -Parts @('AUTH',($token + '.invalid-validation-token'))
        if ($badReply -like '+OK*') { Add-Check 'RESP invalid token rejected' 'FAIL' 'Server accepted a deliberately invalid token on a fresh connection.' }
        else { Add-Check 'RESP invalid token rejected' 'PASS' 'Fresh connection did not accept a deliberately invalid token.' }
    } catch { if ($badConnected) { Add-Check 'RESP invalid token rejected' 'PASS' 'Fresh connected TLS session rejected/closed the invalid AUTH request.' } else { Add-Check 'RESP invalid token rejected' 'FAIL' ('Could not connect for invalid-token test: ' + $_.Exception.Message) } }
    finally { if ($badReader) {$badReader.Dispose()}; if ($badWriter) {$badWriter.Dispose()}; if ($badSsl) {$badSsl.Dispose()}; if ($badTcp) {$badTcp.Dispose()} }

    try {
        $tcp = [System.Net.Sockets.TcpClient]::new(); $plainConnected = $false
        $tcp.ReceiveTimeout = 1800; $tcp.SendTimeout = 1800
        $tcp.Connect($RespTlsHost, $RespTlsPort); $plainConnected = $true
        $stream = $tcp.GetStream()
        $plain = [System.Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n")
        $stream.Write($plain,0,$plain.Length); $stream.Flush()
        $buffer = New-Object byte[] 256
        try { $read = $stream.Read($buffer,0,$buffer.Length) } catch [System.IO.IOException] { $read = 0 } catch [System.Net.Sockets.SocketException] { $read = 0 }
        $response = if ($read -gt 0) { [System.Text.Encoding]::ASCII.GetString($buffer,0,$read) } else { '' }
        if ($response -match '^[+\-\$\*:_]') { Add-Check 'RESP plaintext rejected on TLS port' 'FAIL' 'Plaintext received a RESP-formatted response; TLS-only enforcement may be missing.' }
        else { Add-Check 'RESP plaintext rejected on TLS port' 'PASS' 'Plaintext did not receive a valid RESP response.' }
    } catch { if ($plainConnected) { Add-Check 'RESP plaintext rejection probe' 'PASS' ("TLS port rejected/closed the plaintext probe: " + $_.Exception.Message) } else { Add-Check 'RESP plaintext rejection probe' 'FAIL' ("Could not connect to the TLS port for plaintext rejection test: " + $_.Exception.Message) } }
    finally { if ($tcp) { $tcp.Dispose() } }
}

if ($RaftTlsHost -and $RaftTlsPort -gt 0) {
    if (-not $ClientPfxPath) {
        Add-Check 'Raft mTLS client certificate' 'BLOCKED' 'Provide -ClientPfxPath and the password environment variable to test an authenticated peer handshake.'
    } else {
        $tcp = $null; $ssl = $null
        try {
            $info = Get-TlsCertificateInfo -HostName $RaftTlsHost -Port $RaftTlsPort -ExpectedFingerprint $ExpectedRaftSha256 -PreviousFingerprint $PreviousRaftSha256 -UseClientCertificate
            Add-Check 'Raft mTLS authenticated handshake' 'PASS' ("host={0}; port={1}; peerServerFingerprint={2}; protocol={3}" -f $RaftTlsHost,$RaftTlsPort,$info.Fingerprint,$info.Protocol)
        } catch { Add-Check 'Raft mTLS authenticated handshake' 'FAIL' $_.Exception.Message }
        if ($PreviousClientPfxPath) {
            try {
                $oldInfo = Get-TlsCertificateInfo -HostName $RaftTlsHost -Port $RaftTlsPort -ClientCertificatePathOverride $PreviousClientPfxPath -ClientCertificatePasswordEnvironmentVariable $PreviousClientPfxPasswordEnvironmentVariable -UseClientCertificate
                Add-Check 'Raft mTLS rejects retired client certificate' 'FAIL' ("Previous client certificate was still accepted; presented server fingerprint {0}. Remove old CA trust/revoke the old certificate if retirement is intended." -f $oldInfo.Fingerprint)
            } catch { Add-Check 'Raft mTLS rejects retired client certificate' 'PASS' 'The previous client certificate was rejected (confirm the failure was certificate-related in server logs).' }
        }
        $tcp = [System.Net.Sockets.TcpClient]::new(); $ssl = $null; $connected = $false
        try {
            $tcp.Connect($RaftTlsHost,$RaftTlsPort)
            $connected = $true
            $ssl = New-Object System.Net.Security.SslStream($tcp.GetStream(),$false)
            $ssl.AuthenticateAsClient($RaftTlsHost)
            Add-Check 'Raft mTLS rejects missing client certificate' 'FAIL' 'TLS handshake succeeded without a client certificate.'
        } catch {
            if ($connected) { Add-Check 'Raft mTLS rejects missing client certificate' 'PASS' 'Connection was established, but handshake without a client certificate was rejected.' }
            else { Add-Check 'Raft mTLS rejects missing client certificate' 'FAIL' ('Could not connect to the Raft TLS endpoint for the negative test: ' + $_.Exception.Message) }
        } finally { if ($ssl) { $ssl.Dispose() }; if ($tcp) { $tcp.Dispose() } }
    }
}

if ($PortsExpectedClosedFromClient.Count -gt 0) {
    $hostForFirewall = if ($RespTlsHost) { $RespTlsHost } elseif ($HttpsHealthUrl) { ([Uri]$HttpsHealthUrl).DnsSafeHost } else { $RaftTlsHost }
    foreach ($port in $PortsExpectedClosedFromClient) {
        try {
            $probe = Test-NetConnection -ComputerName $hostForFirewall -Port $port -WarningAction SilentlyContinue
            if ($probe.TcpTestSucceeded) { Add-Check "Client firewall blocks port $port" 'FAIL' "port $port is reachable from this client; confirm it is meant to be private." }
            else { Add-Check "Client firewall blocks port $port" 'PASS' "port $port was not reachable from this client vantage point." }
        } catch { Add-Check "Client firewall probe port $port" 'FAIL' $_.Exception.Message }
    }
}

$report = [ordered]@{
    startedOrFinishedAt=(Get-Date).ToString('o'); computer=$env:COMPUTERNAME; checks=@($checks)
    passed=@($checks | Where-Object Status -eq 'PASS').Count
    failed=@($checks | Where-Object Status -eq 'FAIL').Count
    blocked=@($checks | Where-Object Status -eq 'BLOCKED').Count
    note='Run from an untrusted client network for firewall checks. Certificate rotation must be followed by a second run with -ExpectedHttpsSha256 and -PreviousHttpsSha256. This script cannot deploy/rotate a certificate for you.'
}
$reportDir = Split-Path -Parent $ReportPath
if ($reportDir) { New-Item -ItemType Directory -Force -Path $reportDir | Out-Null }
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $ReportPath
Write-Host "Security report: $ReportPath"
if ($failures -gt 0) { exit 1 }
