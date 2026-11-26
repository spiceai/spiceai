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
    $hint = "If the GitHub API rate limit was exceeded, set `$env:GITHUB_TOKEN and run the installer again."

    try {
        $releaseInfo = Invoke-RestMethod -Uri $url -Headers (Get-GitHubHeader) -ErrorAction Stop
    } catch {
        throw "Failed to resolve the latest Spice.ai CLI release from ${url}: $($_.Exception.Message). $hint"
    }
    if (-not $releaseInfo.tag_name) {
        throw "Failed to resolve the latest Spice.ai CLI release from $url. $hint"
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
# uploaded, only when the release's asset list confirms it has no SHA256SUMS. Sha256 is empty,
# with the Reason, when neither is available. Fatal is set when the release has a SHA256SUMS
# that does not list the asset, or that could not be downloaded.
# Runs a GitHub request, and tries it up to 3 more times, a second apart, when it got no answer
# or a 408, 429 or 5xx, so a brief GitHub error doesn't stop the install. Any other error, such
# as the 404 for the SHA256SUMS of a release without one, is thrown at once. Works the same in
# Windows PowerShell 5.1 and PowerShell 7, whose errors both carry Response.StatusCode.
function Invoke-GitHubRequest {
    param([scriptblock]$Request)
    for ($attempt = 1; ; $attempt++) {
        try {
            return & $Request
        } catch {
            $status = 0
            if ($_.Exception.Response) {
                $status = [int]$_.Exception.Response.StatusCode
            }
            $transient = $status -eq 0 -or $status -eq 408 -or $status -eq 429 -or $status -ge 500
            if (-not $transient -or $attempt -ge 4) {
                throw
            }
            Start-Sleep -Seconds 1
        }
    }
}

function Get-ExpectedSha256 {
    param([string]$Tag, [string]$AssetName, [string]$WorkDir)

    $headers = Get-GitHubHeader
    $sumsUrl = "https://github.com/$spiceOrgName/$spiceRepoName/releases/download/$Tag/SHA256SUMS"
    $sumsFile = Join-Path $WorkDir "SHA256SUMS"
    $sumsFound = $false
    try {
        Invoke-GitHubRequest { Invoke-WebRequest -Uri $sumsUrl -OutFile $sumsFile -Headers $headers -UseBasicParsing -ErrorAction Stop }
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
        $release = Invoke-GitHubRequest { Invoke-RestMethod -Uri "https://api.github.com/repos/$spiceOrgName/$spiceRepoName/releases/tags/$Tag" -Headers $headers -ErrorAction Stop }
    } catch {
        return @{ Sha256 = ""; Reason = "the SHA256SUMS of release $Tag could not be downloaded, and the GitHub API request for its asset sha256 failed (set GITHUB_TOKEN if the API rate limit was exceeded)" }
    }
    # The release lists SHA256SUMS when it has one. Then the download failed for another reason,
    # and the GitHub digest would skip the attestation check, so stop instead.
    if ($release.assets | Where-Object { $_.name -eq "SHA256SUMS" }) {
        return @{ Sha256 = ""; Fatal = $true; Reason = "release $Tag publishes SHA256SUMS, but downloading it failed ($sumsUrl). Try again later" }
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

# Spice skills for AI coding agents (https://github.com/spiceai/skills). The installer offers
# them only in an interactive session: neither CI nor NONINTERACTIVE set, a session that is not a
# service, and a console whose input and output are not redirected. An unattended install, such
# as a CI job or a provisioning script, never asks and never changes an agent's configuration.
$skillsRepo = "spiceai/skills"
$skillsUrl = "https://github.com/spiceai/skills"
# The agents whose own CLI installs the plugin, in the order the installer lists them. Each name
# is also the agent's command. Cursor and the rest install the skills with `npx skills add`,
# which the installer does not run: it fetches and runs a package from npm.
$skillsAgentOrder = @("claude", "codex", "copilot", "grok")

# Whether a person can answer a question.
function Test-Interactive {
    if ($env:CI -or $env:NONINTERACTIVE) {
        return $false
    }
    if (-not [Environment]::UserInteractive) {
        return $false
    }
    try {
        return -not ([Console]::IsInputRedirected -or [Console]::IsOutputRedirected)
    } catch {
        return $false
    }
}

function Get-AgentName {
    param([string]$Agent)
    switch ($Agent) {
        "claude" { return "Claude Code" }
        "codex" { return "Codex" }
        "copilot" { return "GitHub Copilot CLI" }
        "grok" { return "Grok Build" }
    }
}

# The arguments of the agent's commands that install the plugin: Marketplace, which adds the
# spiceai/skills marketplace (none for Grok), then Install.
function Get-AgentSkillCommand {
    param([string]$Agent)
    switch ($Agent) {
        "claude" { return @{ Marketplace = @("plugin", "marketplace", "add", $skillsRepo); Install = @("plugin", "install", "spiceai@spiceai") } }
        "codex" { return @{ Marketplace = @("plugin", "marketplace", "add", $skillsRepo); Install = @("plugin", "add", "spiceai@spiceai") } }
        "copilot" { return @{ Marketplace = @("plugin", "marketplace", "add", $skillsRepo); Install = @("plugin", "install", "spiceai@spiceai") } }
        "grok" { return @{ Marketplace = $null; Install = @("plugin", "install", $skillsRepo, "--trust") } }
    }
}

# Installs the Spice skills plugin for $Agent with the agent's own CLI. The marketplace step may
# report that the marketplace is already added; whether the plugin install succeeds is what
# counts, and a plugin that is already installed is a success. Adds the agent to
# $script:SkillsInstalled when it is.
function Install-AgentSkill {
    param([string]$Agent)
    # A native command's stderr must not stop the script, whatever the caller's preference.
    $ErrorActionPreference = "Continue"
    $name = Get-AgentName $Agent
    $commands = Get-AgentSkillCommand $Agent
    $marketplace = $commands.Marketplace
    $install = $commands.Install
    Write-Output "Installing the Spice skills plugin for $name..."
    # Empty input: a question from the agent's CLI fails it rather than waiting unseen.
    $log = @()
    if ($marketplace) {
        $log += @($null | & $Agent @marketplace 2>&1)
    }
    $log += @($null | & $Agent @install 2>&1)
    $status = $LASTEXITCODE
    $text = ($log | Out-String).Trim()
    if ($status -ne 0 -and $text -match "already installed") {
        Write-Output "The Spice skills plugin is already installed for $name."
        $script:SkillsInstalled += $Agent
    } elseif ($status -ne 0) {
        Write-Warning "Could not install the Spice skills plugin for $name (exit status $status). spice itself is installed. $name printed: $text"
        $retry = @()
        if ($marketplace) {
            $retry += "$Agent $($marketplace -join ' ')"
        }
        $retry += "$Agent $($install -join ' ')"
        Write-Output "To try again, run: $($retry -join '; ')"
    } else {
        Write-Output "Installed the Spice skills plugin for $name."
        $script:SkillsInstalled += $Agent
    }
}

# The agents an answer picks from the listed ones: numbers from the list, separated by commas or
# spaces, "a" for all, or "n" or nothing for none. Valid is false for any other answer.
function Select-SkillAgent {
    param([string]$Answer, [string[]]$Listed)
    $words = @((($Answer + "").ToLowerInvariant() -replace ',', ' ') -split '\s+' | Where-Object { $_ })
    if ($words.Count -eq 0 -or ($words.Count -eq 1 -and $words[0] -in @("n", "no", "none"))) {
        return @{ Valid = $true; Agents = @() }
    }
    if ($words.Count -eq 1 -and $words[0] -in @("a", "all")) {
        return @{ Valid = $true; Agents = $Listed }
    }
    $chosen = @()
    foreach ($word in $words) {
        if ($word -notmatch '^[0-9]{1,3}$') {
            return @{ Valid = $false; Agents = @() }
        }
        $index = [int]$word
        if ($index -lt 1 -or $index -gt $Listed.Count) {
            return @{ Valid = $false; Agents = @() }
        }
        if ($chosen -notcontains $Listed[$index - 1]) {
            $chosen += $Listed[$index - 1]
        }
    }
    return @{ Valid = $true; Agents = $chosen }
}

# Asks which of the agents found on PATH should get the Spice skills plugin, and installs it for
# them. Without a person to ask, prints where to find the skills instead.
function Invoke-SkillOffer {
    $script:SkillsInstalled = @()
    $found = @($skillsAgentOrder | Where-Object { Get-Command $_ -ErrorAction SilentlyContinue })
    if ($found.Count -eq 0) {
        return
    }
    if (-not (Test-Interactive)) {
        Write-Output "To add the Spice skills to your AI coding agents, see $skillsUrl#installation"
        return
    }

    Write-Output ""
    Write-Output "Spice skills teach AI coding agents to set up and use Spice: $skillsUrl"
    Write-Output "Install the Spice skills plugin for:"
    for ($i = 0; $i -lt $found.Count; $i++) {
        Write-Output "  $($i + 1)) $(Get-AgentName $found[$i])"
    }
    Write-Output "  a) All of these"
    Write-Output "  n) None"
    $selection = @{ Valid = $true; Agents = @() }
    for ($try = 1; $try -le 3; $try++) {
        $selection = Select-SkillAgent (Read-Host "Choose one or more, such as 1,2 [n]") $found
        if ($selection.Valid) {
            break
        }
        if ($try -eq 3) {
            Write-Output "No agent chosen."
            $selection = @{ Valid = $true; Agents = @() }
        } else {
            Write-Output "Answer with numbers from 1 to $($found.Count), a for all, or n for none."
        }
    }
    if ($selection.Agents.Count -eq 0) {
        Write-Output "To add the Spice skills later, see $skillsUrl#installation"
        return
    }
    foreach ($agent in $selection.Agents) {
        Install-AgentSkill $agent
    }
    if ($script:SkillsInstalled.Count -gt 0) {
        Write-Output "Start a new session of your agent and ask it to set up Spice."
        if ($script:SkillsInstalled -contains "claude") {
            Write-Output "  In Claude Code: /spiceai:setup"
        }
        if ($script:SkillsInstalled -contains "codex") {
            Write-Output "  In Codex: `$spiceai:setup"
        }
    }
    Write-Output "For Cursor and other agents, see $skillsUrl#installation"
}

Assert-Supported

Install-SpiceCli

# Add Spice CLI to system PATH
$envPath = [Environment]::GetEnvironmentVariable("PATH", [EnvironmentVariableTarget]::User)
$newEnvPath = Add-PathEntry $envPath $spiceCliInstallDir
if ($null -ne $newEnvPath) {
    [Environment]::SetEnvironmentVariable("PATH", $newEnvPath, [EnvironmentVariableTarget]::User)
    Write-Output "Spice CLI directory added to PATH."
} else {
    Write-Output "Spice CLI directory is already in PATH."
}

Write-Output "`nTo get started with Spice.ai, visit https://spiceai.org/docs"

Invoke-SkillOffer
