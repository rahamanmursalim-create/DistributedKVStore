$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

# When invoked from 'mvn clean test', pass --skip-compile to avoid recursive Maven invocation.
# When invoked standalone, the script compiles first via Maven.
if ($args -notcontains "--skip-compile") {
    Write-Host "Compiling project and tests..."
    $mvnCmd = if (Get-Command mvn -ErrorAction SilentlyContinue) { "mvn" } else {
        # Fallback: find Maven in known location
        $candidate = "C:\Users\mursa\tools\apache-maven-3.10.0\bin\mvn.cmd"
        if (Test-Path $candidate) { $candidate } else { throw "Maven not found on PATH or at $candidate" }
    }
    & $mvnCmd test-compile
    if ($LASTEXITCODE -ne 0) {
        throw "Compilation failed. Tests were not run."
    }
}

$testFiles = @(
    Get-ChildItem ".\src\test\java" -Recurse -Filter "*Test.java"
    Get-ChildItem ".\src\main\java\com\codecrafters\redis\distributed" -Filter "*Test.java" -ErrorAction SilentlyContinue
) | Sort-Object FullName -Unique

$results = [System.Collections.Generic.List[object]]::new()

foreach ($file in $testFiles) {
    $packageLine = Select-String -Path $file.FullName -Pattern '^\s*package\s+([\w.]+)\s*;' |
        Select-Object -First 1

    if ($packageLine) {
        $packageName = [regex]::Match(
            $packageLine.Line, '^\s*package\s+([\w.]+)\s*;'
        ).Groups[1].Value
        $className = "$packageName.$($file.BaseName)"
    } else {
        $className = $file.BaseName
    }

    Write-Host "`n===== RUNNING $className =====" -ForegroundColor Cyan

    # Run each test with assertions enabled (-ea).
    # Use target\classes;target\test-classes so both main and test sources are available.
    $testOutput = & java -ea -cp "target\classes;target\test-classes" $className 2>&1
    $exitCode = $LASTEXITCODE

    foreach ($line in $testOutput) {
        Write-Host $line
    }

    $result = if ($exitCode -eq 0) { "PASS" } else { "FAIL" }

    $results.Add([PSCustomObject]@{
        Test   = $className
        Result = $result
    })

    Write-Host "$result : $className"
}

Write-Host "`n===== FINAL TEST SUMMARY =====" -ForegroundColor Yellow
$results | Format-Table -AutoSize

$passed = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed = @($results | Where-Object { $_.Result -eq "FAIL" }).Count

Write-Host "Test classes executed: $($results.Count)"
Write-Host "Passed: $passed"
Write-Host "Failed: $failed"

if ($failed -gt 0) {
    exit 1
}
