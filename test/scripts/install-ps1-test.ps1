# Copyright 2026 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
#
# Tests the download verification in install/Install.ps1 with no network: it loads the
# script's functions without running it, and stubs Invoke-WebRequest, Invoke-RestMethod
# and gh with functions, which PowerShell resolves before cmdlets and executables.
#
# Usage: pwsh test/scripts/install-ps1-test.ps1

$ErrorActionPreference = "Stop"

$scriptPath = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../../install/Install.ps1"))
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($scriptPath, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) {
    throw "Install.ps1 does not parse: $($errors[0].Message)"
}
foreach ($function in $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([ScriptBlock]::Create($function.Extent.Text))
}
# The variables the functions read, as the script sets them.
foreach ($assignment in $ast.EndBlock.Statements | Where-Object { $_ -is [System.Management.Automation.Language.AssignmentStatementAst] }) {
    if ($assignment.Left.VariablePath.UserPath -in @("spiceRepoName", "spiceOrgName", "spiceAttestationSigner")) {
        . ([ScriptBlock]::Create($assignment.Extent.Text))
    }
}

$asset = "spice.exe_windows_x86_64.tar.gz"
$tag = "v0.0.0"
$zero = "0" * 64

# What the stubbed release serves. Set per test.
$script:Release = @{}
$script:GhCalls = @()
# How many times each stubbed URL kind was requested: Sums, Api.
$script:Requests = @{}

# An error like the one Invoke-WebRequest and Invoke-RestMethod throw for an HTTP status, which
# carries the response and its status code.
function New-HttpError {
    param([int]$Status, [string]$Message)
    $exception = [System.Exception]::new("$Status $Message")
    $exception | Add-Member -NotePropertyName Response -NotePropertyValue ([pscustomobject]@{ StatusCode = $Status })
    return $exception
}

function Invoke-WebRequest {
    param([string]$Uri, [string]$OutFile, $Headers, [switch]$UseBasicParsing)
    if ($Uri -like "*/SHA256SUMS") {
        $script:Requests.Sums += 1
        if ($script:Requests.Sums -le $script:Release.SumsTransient) {
            throw (New-HttpError 504 "Gateway Timeout: $Uri")
        }
    }
    if ($Uri -like "*/SHA256SUMS" -and $script:Release.SumsUnreachable) {
        throw (New-HttpError 503 "Service Unavailable: $Uri")
    }
    if ($Uri -like "*/SHA256SUMS" -and $null -ne $script:Release.Sums) {
        Set-Content -Path $OutFile -Value $script:Release.Sums -NoNewline
        return
    }
    throw (New-HttpError 404 "Not Found: $Uri")
}

function Invoke-RestMethod {
    param([string]$Uri, $Headers)
    $script:Requests.Api += 1
    if ($script:Requests.Api -le $script:Release.ApiTransient) {
        throw (New-HttpError 504 "Gateway Timeout: $Uri")
    }
    if ($script:Release.ApiFails) {
        throw (New-HttpError 403 "rate limit exceeded")
    }
    # A release lists SHA256SUMS among its assets exactly when it has one.
    $assets = @(
        [pscustomobject]@{ name = "cuda_$asset"; digest = "sha256:" + ("7" * 64) },
        [pscustomobject]@{ name = $asset; digest = $script:Release.Digest }
    )
    if ($null -ne $script:Release.Sums -or $script:Release.SumsUnreachable) {
        $assets += [pscustomobject]@{ name = "SHA256SUMS"; digest = "sha256:" + ("8" * 64) }
    }
    return [pscustomobject]@{ assets = $assets }
}

function gh {
    $script:GhCalls += ($args -join " ")
    if ($args[0] -eq "auth") {
        $global:LASTEXITCODE = if ($script:Release.Gh -in @("pass", "fail")) { 0 } else { 1 }
    } elseif ($args[0] -eq "attestation") {
        $global:LASTEXITCODE = if ($script:Release.Gh -eq "pass") { 0 } else { 1 }
        if ($script:Release.Gh -ne "pass") { "verification failed" }
    }
}

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("install-ps1-test-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -Path $work -ItemType Directory > $null
$download = Join-Path $work $asset
Set-Content -Path $download -Value "archive" -NoNewline
$digest = (Get-FileHash -Algorithm SHA256 -Path $download).Hash.ToLowerInvariant()

$failures = 0
function Test-Case {
    param([string]$Name, [hashtable]$Release, [string]$Expect, [string]$Message, [switch]$AllowUnverified)
    $script:Release = $Release
    $script:GhCalls = @()
    $script:Requests = @{ Sums = 0; Api = 0 }
    $env:SPICE_INSTALL_ALLOW_UNVERIFIED = if ($AllowUnverified) { "1" } else { $null }
    $output = ""
    $outcome = "installed"
    try {
        $output = (Assert-VerifiedDownload -Path $download -AssetName $asset -Tag $tag -WorkDir $work 3>&1 | Out-String)
    } catch {
        $outcome = "stopped"
        $output = $_.Exception.Message
    }
    if ($outcome -eq $Expect -and $output -like "*$Message*") {
        Write-Output "[PASS] $Name"
    } else {
        Write-Output "[FAIL] $Name`n  expected $Expect with '$Message'`n  got $outcome with '$output'"
        $script:failures += 1
    }
}

$signerArgs = "--repo spiceai/spiceai --cert-identity-regex $spiceAttestationSigner --deny-self-hosted-runners"

Test-Case "verifies against SHA256SUMS" @{ Sums = "$digest  $asset`n" } "installed" "against the SHA256SUMS of release v0.0.0"
Test-Case "skips the attestation check when gh is logged out" @{ Sums = "$digest  $asset`n" } "installed" "Skipped the build attestation check"
Test-Case "rejects a SHA256SUMS mismatch" @{ Sums = "$zero  $asset`n" } "stopped" "Checksum mismatch for $asset"
Test-Case "rejects an asset missing from SHA256SUMS" @{ Sums = "$digest  other.tar.gz`n" } "stopped" "does not list it"
Test-Case "falls back to the GitHub digest" @{ Digest = "sha256:$digest" } "installed" "against the sha256 GitHub recorded for $asset"
Test-Case "rejects a GitHub digest mismatch" @{ Digest = "sha256:$zero" } "stopped" "Checksum mismatch for $asset"
Test-Case "fails closed without a checksum" @{ Digest = $null } "stopped" "SPICE_INSTALL_ALLOW_UNVERIFIED"
Test-Case "fails closed when the API request fails" @{ ApiFails = $true } "stopped" "the GitHub API request for its asset sha256 failed"
Test-Case "opt-out installs an unverifiable download" @{ Digest = $null } "installed" "SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set" -AllowUnverified
Test-Case "opt-out never installs a mismatch" @{ Sums = "$zero  $asset`n" } "stopped" "Checksum mismatch" -AllowUnverified
Test-Case "checks the attestation when gh is logged in" @{ Sums = "$digest  $asset`n"; Gh = "pass" } "installed" "Verified the build attestation of $asset"
if (-not ($script:GhCalls | Where-Object { $_ -like "attestation verify * $signerArgs" })) {
    Write-Output "[FAIL] gh attestation verify was called with $signerArgs`n  calls: $($script:GhCalls -join '; ')"
    $failures += 1
}
Test-Case "rejects a failed attestation" @{ Sums = "$digest  $asset`n"; Gh = "fail" } "stopped" "did not verify: verification failed"
Test-Case "asks gh nothing about a release without SHA256SUMS" @{ Digest = "sha256:$digest"; Gh = "pass" } "installed" "GitHub recorded"
if ($script:GhCalls.Count -ne 0) {
    Write-Output "[FAIL] gh was called for a release without SHA256SUMS: $($script:GhCalls -join '; ')"
    $failures += 1
}

Test-Case "stops when a listed SHA256SUMS cannot be downloaded" @{ SumsUnreachable = $true; Digest = "sha256:$digest"; Gh = "fail" } "stopped" "publishes SHA256SUMS, but downloading it failed"
Test-Case "opt-out does not skip a listed SHA256SUMS" @{ SumsUnreachable = $true; Digest = "sha256:$digest" } "stopped" "publishes SHA256SUMS, but downloading it failed" -AllowUnverified

# A brief GitHub error is retried; a 404 or a rate limit is not.
function Assert-RequestCount {
    param([string]$Name, [int]$Sums, [int]$Api)
    if ($script:Requests.Sums -ne $Sums -or $script:Requests.Api -ne $Api) {
        Write-Output "[FAIL] $Name`n  expected $Sums SHA256SUMS and $Api API request(s), got $($script:Requests.Sums) and $($script:Requests.Api)"
        $script:failures += 1
    }
}
Test-Case "retries a SHA256SUMS download that got a 504" @{ Sums = "$digest  $asset`n"; SumsTransient = 1 } "installed" "against the SHA256SUMS of release v0.0.0"
Assert-RequestCount "retries a SHA256SUMS download that got a 504" 2 0
Test-Case "retries a GitHub API request that got a 504" @{ Digest = "sha256:$digest"; ApiTransient = 1 } "installed" "against the sha256 GitHub recorded for $asset"
Assert-RequestCount "retries a GitHub API request that got a 504" 1 2
Test-Case "does not retry a missing SHA256SUMS" @{ Digest = "sha256:$digest" } "installed" "GitHub recorded"
Assert-RequestCount "does not retry a missing SHA256SUMS" 1 1
Test-Case "does not retry a rate-limited API request" @{ ApiFails = $true } "stopped" "the GitHub API request for its asset sha256 failed"
Assert-RequestCount "does not retry a rate-limited API request" 1 1

Remove-Item -Path $work -Recurse -Force
$env:SPICE_INSTALL_ALLOW_UNVERIFIED = $null
if ($failures -gt 0) {
    Write-Output "$failures failed"
    exit 1
}
Write-Output "All Install.ps1 verification tests passed"
# The gh stub leaves $LASTEXITCODE set, and GitHub Actions' pwsh shell exits with it.
exit 0
