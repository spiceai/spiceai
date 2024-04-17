# PowerShell Script to Install Spice CLI on Windows

$spiceBin = ".spice\bin"
$spiceCliInstallDir = Join-Path $HOME $spiceBin
$spiceRepoName = "spiceai"
$spiceOrgName = "spiceai"
$spiceCliFileName = "spice.exe"
$spiceCliFullPath= Join-Path $spiceCliInstallDir $spiceCliFileName

# Ensure the installation directory and auth file exist
New-Item -Path $spiceCliInstallDir -ItemType Directory -Force

function Get-LatestRelease {
    $url = "https://api.github.com/repos/$spiceOrgName/$spiceRepoName/releases/latest"
    $releaseInfo = Invoke-RestMethod -Uri $url
    return $releaseInfo.tag_name
}

# Function to download and install Spice CLIv
function Download-And-Install-Spice {
    Write-Host "Checking the latest Spice version..."

    $latestReleaseTag = Get-LatestRelease

    Write-Host "Installing Spice $latestReleaseTag"

    $artifactName = "${spiceCliFileName}"
    $downloadUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$latestReleaseTag/$artifactName"
    $tempPath = [System.IO.Path]::GetTempPath()
    $tempFile = Join-Path $tempPath $artifactName

    Write-Host "Downloading Spice CLI from $downloadUrl..."
    Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile

    Move-Item -Path (Join-Path $tempPath $spiceCliFilename) -Destination $spiceCliFullPath -Force

    # Temporary download Spice runtime as `spice upgrade` is not currently working on Windows.
    $runtimeDownloadUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$latestReleaseTag/spiced.exe"
    $runtimeInstallPath = Join-Path $spiceCliInstallDir "spiced.exe"
    Write-Host "Downloading Spice Runtime from $runtimeDownloadUrl..."
    Invoke-WebRequest -Uri $runtimeDownloadUrl -OutFile $runtimeInstallPath

    Temporary workaround for spice CLI to work on Windows (expect runtime binary as spiced instead of spiced.exe).
    $emptySpicedPath = Join-Path $spiceCliInstallDir "spiced"
    $latestReleaseTag  | Out-File -FilePath $emptySpicedPath

    if (Test-Path $spiceCliFullPath) {
        Write-Host "Spice CLI installed into $spiceCliInstallDir successfully."
    } else {
        Write-Host "Failed to install Spice CLI."
        exit
    }
}

Download-And-Install-Spice

# Add Spice CLI to system PATH
$envPath = [Environment]::GetEnvironmentVariable("PATH", [EnvironmentVariableTarget]::User)
if (-not $envPath.Contains($spiceCliInstallDir)) {
    [Environment]::SetEnvironmentVariable("PATH", $envPath + ";$spiceCliInstallDir", [EnvironmentVariableTarget]::User)
    Write-Host "Spice CLI directory added to PATH."
} else {
    Write-Host "Spice CLI directory is already in PATH."
}

Write-Host "`nTo get started with Spice.ai, visit https://docs.spiceai.org"
