# PowerShell Script to Install Spice CLI on Windows

$spiceBin = ".spice\bin"
$spiceCliInstallDir = Join-Path $HOME $spiceBin
$spiceRepoName = "spiceai"
$spiceOrgName = "spiceai"
$spiceCliFileName = "spice.exe"
$spiceCliFullPath= Join-Path $spiceCliInstallDir $spiceCliFileName

# Release attestations are signed by build_release.yml running on a release branch.
$spiceAttestationSigner = '^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/'

# Ensure the installation directory exists
New-Item -Path $spiceCliInstallDir -ItemType Directory -Force > $null

function Get-GitHubHeader {
    $headers = @{}
    if ($env:GITHUB_TOKEN) {
        $headers["Authorization"] = "token $env:GITHUB_TOKEN"
    }
    return $headers
}

function Get-LatestRelease {
    $url = "https://api.github.com/repos/$spiceOrgName/$spiceRepoName/releases/latest"

    $releaseInfo = Invoke-RestMethod -Uri $url -Headers (Get-GitHubHeader) -ErrorAction Stop
    if (-not $releaseInfo.tag_name) {
        throw "Failed to resolve the latest Spice.ai CLI release from $url."
    }
    return $releaseInfo.tag_name
}

function Assert-Supported {
    $osInfo = Get-CimInstance Win32_OperatingSystem
    $systemInfo = Get-CimInstance CIM_ComputerSystem

    $currentOs = $osInfo.Caption
    $currentArch = $systemInfo.SystemType

    if ($currentOs -notlike "*Windows*") {
        throw "This script is only supported on Windows operating systems."
    }

    Write-Output "System architecture is $currentArch"
}

# The sha256 a release records for one of its assets: the release's SHA256SUMS, or for
# releases published before SHA256SUMS, the sha256 GitHub recorded when the asset was
# uploaded. Sha256 is empty, with the Reason, when neither is available. Fatal is set when
# the release has a SHA256SUMS that does not list the asset.
function Get-ExpectedSha256 {
    param([string]$Tag, [string]$AssetName, [string]$WorkDir)

    $headers = Get-GitHubHeader
    $sumsUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$Tag/SHA256SUMS"
    $sumsFile = Join-Path $WorkDir "SHA256SUMS"
    $sumsFound = $false
    try {
        Invoke-WebRequest -Uri $sumsUrl -OutFile $sumsFile -Headers $headers -UseBasicParsing -ErrorAction Stop
        $sumsFound = $true
    } catch {
        Write-Verbose "No SHA256SUMS at ${sumsUrl}: $($_.Exception.Message)"
    }

    if ($sumsFound) {
        foreach ($line in Get-Content -Path $sumsFile) {
            $fields = $line -split '\s+', 2
            if ($fields.Count -eq 2 -and $fields[1].TrimStart('*') -eq $AssetName) {
                return @{ Sha256 = $fields[0].ToLowerInvariant(); Source = "SHA256SUMS of release $Tag"; Attested = $true }
            }
        }
        return @{ Sha256 = ""; Fatal = $true; Reason = "the SHA256SUMS of release $Tag does not list it" }
    }

    try {
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$spiceOrgName/$spiceRepoName/releases/tags/$Tag" -Headers $headers -ErrorAction Stop
    } catch {
        return @{ Sha256 = ""; Reason = "release $Tag has no SHA256SUMS, and the GitHub API request for its asset sha256 failed (set GITHUB_TOKEN if the API rate limit was exceeded)" }
    }
    $asset = $release.assets | Where-Object { $_.name -eq $AssetName } | Select-Object -First 1
    if ($asset -and $asset.digest -match '^sha256:([0-9a-fA-F]{64})$') {
        return @{ Sha256 = $Matches[1].ToLowerInvariant(); Source = "sha256 GitHub recorded for $AssetName in release $Tag"; Attested = $false }
    }
    return @{ Sha256 = ""; Reason = "release $Tag has no SHA256SUMS, and GitHub recorded no sha256 for $AssetName" }
}

# Stops the install, before anything is extracted, unless a downloaded release asset matches
# the sha256 its release records. A release with SHA256SUMS also has build attestations,
# which are checked when the GitHub CLI is logged in. SPICE_INSTALL_ALLOW_UNVERIFIED=1
# installs an asset that cannot be checked at all; it never installs one whose check fails.
function Assert-VerifiedDownload {
    param([string]$Path, [string]$AssetName, [string]$Tag, [string]$WorkDir)

    $expected = Get-ExpectedSha256 -Tag $Tag -AssetName $AssetName -WorkDir $WorkDir
    if ($expected.Fatal) {
        throw "Cannot install ${AssetName}: $($expected.Reason). Nothing was installed."
    }
    if (-not $expected.Sha256) {
        if ($env:SPICE_INSTALL_ALLOW_UNVERIFIED -eq "1") {
            Write-Warning "Installing $AssetName unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set: $($expected.Reason)."
            return
        }
        throw "Cannot verify ${AssetName}: $($expected.Reason). To install it without verification, set `$env:SPICE_INSTALL_ALLOW_UNVERIFIED = '1' and run the installer again. Nothing was installed."
    }

    $actual = (Get-FileHash -Algorithm SHA256 -Path $Path).Hash.ToLowerInvariant()
    if ($actual -ne $expected.Sha256) {
        throw "Checksum mismatch for ${AssetName}: expected sha256 $($expected.Sha256) ($($expected.Source)), downloaded $actual. The download is corrupt or was altered. Nothing was installed."
    }
    Write-Output "Verified the sha256 of $AssetName against the $($expected.Source)."

    if ($expected.Attested) {
        $loggedIn = $false
        if (Get-Command gh -ErrorAction SilentlyContinue) {
            & gh auth status *> $null
            $loggedIn = ($LASTEXITCODE -eq 0)
        }
        if ($loggedIn) {
            $log = & gh attestation verify $Path --repo "$spiceOrgName/$spiceRepoName" --cert-identity-regex $spiceAttestationSigner --deny-self-hosted-runners 2>&1
            if ($LASTEXITCODE -ne 0) {
                throw "The build attestation of $AssetName did not verify: $($log -join ' ') Nothing was installed."
            }
            Write-Output "Verified the build attestation of $AssetName."
        } else {
            Write-Output "Skipped the build attestation check of ${AssetName}: the GitHub CLI (gh) is not installed or not logged in."
        }
    }
}

function Install-SpiceCli {
    Write-Output "Checking the latest Spice version..."

    $latestReleaseTag = Get-LatestRelease
    $arch = "x86_64"

    Write-Output "Installing Spice $latestReleaseTag"

    $artifactName="${spiceCliFileName}_windows_$arch.tar.gz"
    $downloadUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$latestReleaseTag/$artifactName"
    $workDir = Join-Path ([System.IO.Path]::GetTempPath()) ("spice-install-" + [System.Guid]::NewGuid().ToString("N"))
    New-Item -Path $workDir -ItemType Directory -Force > $null
    $tempFile = Join-Path $workDir $artifactName

    try {
        Write-Output "Downloading Spice CLI from $downloadUrl..."
        Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile -Headers (Get-GitHubHeader) -UseBasicParsing -ErrorAction Stop
        Assert-VerifiedDownload -Path $tempFile -AssetName $artifactName -Tag $latestReleaseTag -WorkDir $workDir

        tar -xf $tempFile -C $workDir
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to extract $artifactName."
        }

        Move-Item -Path (Join-Path $workDir $spiceCliFileName) -Destination $spiceCliFullPath -Force
    } finally {
        Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    if (Test-Path $spiceCliFullPath) {
        Write-Output "Spice CLI installed into $spiceCliInstallDir successfully."
    } else {
        throw "Failed to install Spice CLI."
    }
}

Assert-Supported

Install-SpiceCli

# Add Spice CLI to system PATH
$envPath = [Environment]::GetEnvironmentVariable("PATH", [EnvironmentVariableTarget]::User)
if (-not $envPath.Contains($spiceCliInstallDir)) {
    [Environment]::SetEnvironmentVariable("PATH", $envPath + ";$spiceCliInstallDir", [EnvironmentVariableTarget]::User)
    Write-Output "Spice CLI directory added to PATH."
} else {
    Write-Output "Spice CLI directory is already in PATH."
}

Write-Output "`nTo get started with Spice.ai, visit https://spiceai.org/docs"
