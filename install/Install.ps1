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

function Verify-Supported {
    $osInfo = Get-CimInstance Win32_OperatingSystem
    $systemInfo = Get-CimInstance CIM_ComputerSystem

    $currentOs = $osInfo.Caption
    $currentArch = $systemInfo.SystemType

    if ($currentOs -notlike "*Windows*") {
        Write-Host "This script is only supported on Windows operating systems."
        exit 1
    }

    Write-Host "System architecture is $currentArch"
}

# Function to download and install Spice CLIv
function Download-And-Install-Spice {
    Write-Host "Checking the latest Spice version..."

    $latestReleaseTag = Get-LatestRelease
    $arch = "x86_64"

    Write-Host "Installing Spice $latestReleaseTag"

    $artifactName="${spiceCliFileName}_windows_$arch.tar.gz"
    $downloadUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$latestReleaseTag/$artifactName"
    $tempPath = [System.IO.Path]::GetTempPath()
    $tempFile = Join-Path $tempPath $artifactName

    Write-Host "Downloading Spice CLI from $downloadUrl..."
    Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile
    tar -xf $tempFile -C $tempPath

    Move-Item -Path (Join-Path $tempPath $spiceCliFilename) -Destination $spiceCliFullPath -Force

    # Download the Spice runtime
    $runtimeInstallPath = Join-Path $spiceCliInstallDir "spiced.exe"
    $runtimeDownloadUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$latestReleaseTag/spiced.exe_windows_$arch.tar.gz"
    $tempFile = Join-Path $tempPath "spiced.exe_windows_$arch.tar.gz"
    Write-Host "Downloading Spice Runtime from $runtimeDownloadUrl..."
    Invoke-WebRequest -Uri $runtimeDownloadUrl -OutFile $tempFile
    tar -xf $tempFile -C $tempPath

    Move-Item -Path (Join-Path $tempPath "spiced.exe") -Destination $runtimeInstallPath -Force

    # Temporary workaround for spice CLI to work on Windows (expect runtime binary as spiced instead of spiced.exe).
    $latestReleaseTag  | Out-File -FilePath (Join-Path $spiceCliInstallDir "spiced")

    if (Test-Path $spiceCliFullPath) {
        Write-Host "Spice CLI installed into $spiceCliInstallDir successfully."
    } else {
        Write-Host "Failed to install Spice CLI."
        exit
    }
}

Verify-Supported

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
