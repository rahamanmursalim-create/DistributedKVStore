param(
    [Parameter(Mandatory = $true)][string]$HostName,
    [Parameter(Mandatory = $true)][int]$HttpsPort,
    [Parameter(Mandatory = $true)][int]$RespTlsPort,
    [Parameter(Mandatory = $true)][string]$ExpectedThumbprint,
    [Parameter(Mandatory = $true)][string]$KeystorePath,
    [Parameter(Mandatory = $true)][string]$RaftPeerAddress,
    [Parameter(Mandatory = $true)][int]$RaftPeerPort,
    [string]$TokenEnvironmentVariable = 'KV_CLIENT_TOKEN'
)
$ErrorActionPreference = 'Stop'
$token = [Environment]::GetEnvironmentVariable($TokenEnvironmentVariable)
if ([string]::IsNullOrWhiteSpace($token) -or $token -match '\s') { throw "$TokenEnvironmentVariable must contain a non-empty token without whitespace." }
if (-not (Test-Path -LiteralPath $KeystorePath -PathType Leaf)) { throw "Keystore not found: $KeystorePath" }
$keytool = Join-Path (Split-Path (Get-Command java.exe -ErrorAction Stop).Source) 'keytool.exe'
$password = [Environment]::GetEnvironmentVariable('KV_TLS_PASSWORD')
if ([string]::IsNullOrWhiteSpace($password)) { throw 'Set KV_TLS_PASSWORD in the current process environment.' }
$storeType = if ($env:KV_TLS_STORE_TYPE) { $env:KV_TLS_STORE_TYPE } else { 'PKCS12' }
$listing = & $keytool -list -v -keystore $KeystorePath -storetype $storeType -storepass $password 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { throw "keytool could not load the configured identity store: $listing" }
if ($listing -match 'Entry type: trustedCertEntry') { throw 'Configured TLS keystore has no private-key identity entry.' }
$expected = ($ExpectedThumbprint -replace '\s','').ToUpperInvariant()
$script:ObservedThumbprint = $null
$handler = [System.Net.Http.HttpClientHandler]::new()
$handler.ServerCertificateCustomValidationCallback = {
    param($request, $certificate, $chain, $errors)
    if ($certificate) { $script:ObservedThumbprint = $certificate.GetCertHashString().ToUpperInvariant() }
    return $errors -eq [System.Net.Security.SslPolicyErrors]::None
}
$client = [System.Net.Http.HttpClient]::new($handler)
try {
    $base = "https://${HostName}:$HttpsPort"
    $anonymous = $client.GetAsync("$base/api/health").GetAwaiter().GetResult()
    if ([int]$anonymous.StatusCode -ne 401) { throw "Unauthenticated HTTPS request expected 401, got $([int]$anonymous.StatusCode)." }
    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Get, "$base/api/health")
    $request.Headers.Authorization = [System.Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer', $token)
    $authorized = $client.SendAsync($request).GetAwaiter().GetResult()
    if ([int]$authorized.StatusCode -ne 200) { throw "Authenticated HTTPS request expected 200, got $([int]$authorized.StatusCode)." }
    if ($script:ObservedThumbprint -ne $expected) { throw "Served certificate thumbprint mismatch. expected=$expected actual=$script:ObservedThumbprint" }
} finally { $client.Dispose() }

$plainRejected = $false
try { $plain = [System.Net.Http.HttpClient]::new(); $response = $plain.GetAsync("http://${HostName}:$HttpsPort/api/health").GetAwaiter().GetResult(); $plain.Dispose() }
catch { $plainRejected = $true }
if (-not $plainRejected) { throw 'HTTPS listener accepted an HTTP plaintext request.' }

if ($RaftPeerAddress -in @('127.0.0.1','localhost') -or $HostName -in @('127.0.0.1','localhost')) { throw 'Deployment verification must use non-loopback addresses.' }
$raft = Test-NetConnection -ComputerName $RaftPeerAddress -Port $RaftPeerPort -WarningAction SilentlyContinue
if (-not $raft.TcpTestSucceeded) { throw "Raft peer route/firewall check failed: $RaftPeerAddress`:$RaftPeerPort" }

# Verify the RESP endpoint performs TLS before accepting AUTH/PING.
$tcp = [System.Net.Sockets.TcpClient]::new()
$ssl = $null
try {
    $tcp.Connect($HostName, $RespTlsPort)
    $ssl = [System.Net.Security.SslStream]::new($tcp.GetStream(), $false)
    $ssl.AuthenticateAsClient($HostName)
    $certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($ssl.RemoteCertificate)
    if ($certificate.Thumbprint.ToUpperInvariant() -ne $expected) { throw "RESP certificate thumbprint mismatch: $($certificate.Thumbprint)" }
    $writer = [System.IO.StreamWriter]::new($ssl, [System.Text.Encoding]::ASCII, 1024, $true); $writer.AutoFlush = $true
    $reader = [System.IO.StreamReader]::new($ssl, [System.Text.Encoding]::ASCII, $false, 1024, $true)
    $writer.Write("*2`r`n`$4`r`nAUTH`r`n`$$($token.Length)`r`n$token`r`n")
    if ($reader.ReadLine() -ne '+OK') { throw 'RESP AUTH did not return +OK over TLS.' }
    $writer.Write("*1`r`n`$4`r`nPING`r`n")
    if ($reader.ReadLine() -ne '+PONG') { throw 'RESP PING did not return +PONG over TLS after AUTH.' }
} finally { if ($ssl) { $ssl.Dispose() }; $tcp.Dispose() }

Write-Host "Deployment security checks passed for $HostName. HTTPS/RESP TLS, bearer/AUTH, certificate fingerprint and Raft network reachability verified."
