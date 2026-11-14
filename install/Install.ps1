# PowerShell Script to Install Spice CLI on Windows

$spiceBin = ".spice\bin"
$spiceCliInstallDir = Join-Path $HOME $spiceBin
$spiceRepoName = "spiceai"
$spiceOrgName = "spiceai"
$spiceCliFileName = "spice.exe"
$spiceCliFullPath= Join-Path $spiceCliInstallDir $spiceCliFileName

# Ensure the installation directory exists
New-Item -Path $spiceCliInstallDir -ItemType Directory -Force > $null

function Get-LatestRelease {
    $url = "https://api.github.com/repos/$spiceOrgName/$spiceRepoName/releases/latest"

    $headers = @{}
    if ($env:GITHUB_TOKEN) {
        $headers["Authorization"] = "token $env:GITHUB_TOKEN"
    }

    $releaseInfo = Invoke-RestMethod -Uri $url -Headers $headers
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

    $headers = @{}
    if ($env:GITHUB_TOKEN) {
        $headers["Authorization"] = "token $env:GITHUB_TOKEN"
    }

    Write-Host "Downloading Spice CLI from $downloadUrl..."
    Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile -Headers $headers
    tar -xf $tempFile -C $tempPath

    Move-Item -Path (Join-Path $tempPath $spiceCliFilename) -Destination $spiceCliFullPath -Force

    if (Test-Path $spiceCliFullPath) {
        Write-Host "Spice CLI installed into $spiceCliInstallDir successfully."
    } else {
        Write-Host "Failed to install Spice CLI."
        exit
    }
}

# Returns $pathValue with $directory appended, or $null when $directory is already one of its
# entries. An unset or empty value has no entries. Entries match whole, ignoring case and a
# trailing backslash, so C:\x\bin2 does not count as C:\x\bin. Existing text is kept as it is.
function Add-PathEntry {
    param([string]$pathValue, [string]$directory)

    $wanted = $directory.TrimEnd('\')
    foreach ($entry in ($pathValue -split ';')) {
        if ($entry -and ($entry.TrimEnd('\') -ieq $wanted)) {
            return $null
        }
    }
    if ([string]::IsNullOrEmpty($pathValue)) {
        return $directory
    }
    if ($pathValue.EndsWith(';')) {
        return $pathValue + $directory
    }
    return "$pathValue;$directory"
}

Verify-Supported

Download-And-Install-Spice

# Add Spice CLI to system PATH
$envPath = [Environment]::GetEnvironmentVariable("PATH", [EnvironmentVariableTarget]::User)
$newEnvPath = Add-PathEntry $envPath $spiceCliInstallDir
if ($null -ne $newEnvPath) {
    [Environment]::SetEnvironmentVariable("PATH", $newEnvPath, [EnvironmentVariableTarget]::User)
    Write-Host "Spice CLI directory added to PATH."
} else {
    Write-Host "Spice CLI directory is already in PATH."
}

Write-Host "`nTo get started with Spice.ai, visit https://spiceai.org/docs"
