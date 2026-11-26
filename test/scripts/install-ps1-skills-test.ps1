# Copyright 2026 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
#
# Tests how install/Install.ps1 offers the Spice skills plugin to AI coding agents, with no network
# and no change to the machine: it loads the script's functions without running it, and stubs
# Read-Host and, where a test needs a person at the console, Test-Interactive, with functions,
# which PowerShell resolves before cmdlets. Each agent is a stub script in a directory that is all
# of PATH while the offer runs, so no agent installed on the machine is ever found or run.
#
# Usage: pwsh test/scripts/install-ps1-skills-test.ps1

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
    if ($assignment.Left.VariablePath.UserPath -in @("skillsRepo", "skillsUrl", "skillsAgentOrder")) {
        . ([ScriptBlock]::Create($assignment.Extent.Text))
    }
}
# The script offers the skills after it installs spice.
$statements = @($ast.EndBlock.Statements | ForEach-Object { $_.Extent.Text })
if ($statements[-1] -ne "Invoke-SkillOffer") {
    throw "Install.ps1 no longer ends by offering the skills, as this test expects"
}

$realTestInteractive = ${function:Test-Interactive}

# What Read-Host answers. Set per test.
$script:Answers = @()
$script:Asked = 0

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("install-ps1-skills-test-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -Path $work -ItemType Directory > $null
$calls = Join-Path $work "calls.txt"

function Read-Host {
    param([string]$Prompt)
    $script:Asked += 1
    if ($script:Answers.Count -eq 0) {
        return ""
    }
    $answer = $script:Answers[0]
    $script:Answers = @($script:Answers | Select-Object -Skip 1)
    return $answer
}

# Returns a new directory holding a stub script for each agent in $Agents. A stub records its
# arguments to $calls, and its plugin install fails when the agent is $Fail, or fails as already
# installed when it is $Already.
function New-AgentDir {
    param([string[]]$Agents, [string]$Fail, [string]$Already)
    $dir = Join-Path $work ([System.Guid]::NewGuid().ToString("N"))
    New-Item -Path $dir -ItemType Directory > $null
    foreach ($agent in $Agents) {
        $stub = @"
Add-Content -Path '$calls' -Value ("$agent " + (`$args -join ' '))
if ((`$args -join ' ') -like '*marketplace*') { 'marketplace added'; exit 0 }
if ('$agent' -eq '$Fail') { 'boom: network unreachable'; exit 1 }
if ('$agent' -eq '$Already') { "Error: repo 'skills-734724f4' already installed"; exit 1 }
'installed'
exit 0
"@
        Set-Content -Path (Join-Path $dir "$agent.ps1") -Value $stub
    }
    return $dir
}

$failures = 0
function Test-Offer {
    param(
        [string]$Name, [string[]]$Agents, [string[]]$Answers, [bool]$Interactive,
        [string[]]$ExpectCalls, [string[]]$ExpectOutput, [string[]]$ExpectMissing = @(),
        [string]$Fail = "", [string]$Already = ""
    )
    $dir = New-AgentDir $Agents $Fail $Already
    Set-Content -Path $calls -Value $null
    $script:Answers = $Answers
    $script:Asked = 0
    $answer = $Interactive
    Set-Item -Path function:script:Test-Interactive -Value { $answer }.GetNewClosure()
    $savedPath = $env:PATH
    try {
        $env:PATH = $dir
        $output = (Invoke-SkillOffer 3>&1 | Out-String)
    } finally {
        $env:PATH = $savedPath
    }
    $made = @(Get-Content -Path $calls | Where-Object { $_ })
    $problems = @()
    if (($made -join "`n") -ne ($ExpectCalls -join "`n")) {
        $problems += "expected agent commands:`n    $($ExpectCalls -join "`n    ")`n  got:`n    $($made -join "`n    ")"
    }
    foreach ($text in $ExpectOutput) {
        if ($output -notlike "*$text*") { $problems += "missing from the output: $text" }
    }
    foreach ($text in $ExpectMissing) {
        if ($output -like "*$text*") { $problems += "unexpected in the output: $text" }
    }
    if ($problems.Count -eq 0) {
        Write-Output "[PASS] $Name"
    } else {
        Write-Output "[FAIL] $Name`n  $($problems -join "`n  ")`n  output:`n$output"
        $script:failures += 1
    }
}

$claude = @("claude plugin marketplace add spiceai/skills", "claude plugin install spiceai@spiceai")
$codex = @("codex plugin marketplace add spiceai/skills", "codex plugin add spiceai@spiceai")
$copilot = @("copilot plugin marketplace add spiceai/skills", "copilot plugin install spiceai@spiceai")
$grok = @("grok plugin install spiceai/skills --trust")
$menu = "Install the Spice skills plugin for:"

Test-Offer "never asks without a person at the console" @("claude", "codex") @("a") $false @() `
    @("To add the Spice skills to your AI coding agents, see https://github.com/spiceai/skills#installation") @($menu)
if ($script:Asked -ne 0) { Write-Output "[FAIL] Read-Host was called without a person at the console"; $failures += 1 }
Test-Offer "asks nothing when no agent is installed" @() @("a") $true @() @() @("Spice skills")
Test-Offer "installs for the chosen agent" @("claude", "codex", "grok") @("1") $true $claude `
    @("1) Claude Code", "2) Codex", "3) Grok Build", "Installed the Spice skills plugin for Claude Code.", "In Claude Code: /spiceai:setup")
Test-Offer "installs for every agent" @("claude", "codex", "copilot", "grok") @("a") $true ($claude + $codex + $copilot + $grok) `
    @("Installed the Spice skills plugin for GitHub Copilot CLI.")
Test-Offer "installs for several agents, in the order chosen" @("claude", "codex", "grok") @("3, 2") $true ($grok + $codex) `
    @("In Codex: `$spiceai:setup")
Test-Offer "defaults to none" @("claude", "codex") @("") $true @() @("To add the Spice skills later")
Test-Offer "asks again after an invalid answer" @("claude", "codex") @("9", "2") $true $codex `
    @("Answer with numbers from 1 to 2, a for all, or n for none.")
Test-Offer "gives up after three invalid answers" @("claude") @("x", "y", "z", "1") $true @() @("No agent chosen.")
Test-Offer "an agent that fails does not stop the others" @("claude", "codex") @("a") $true ($claude + $codex) `
    @("Could not install the Spice skills plugin for Claude Code (exit status 1). spice itself is installed.", "boom: network unreachable",
        "To try again, run: claude plugin marketplace add spiceai/skills; claude plugin install spiceai@spiceai",
        "Installed the Spice skills plugin for Codex.") -Fail "claude"
Test-Offer "a plugin already installed is not a failure" @("grok") @("1") $true $grok `
    @("The Spice skills plugin is already installed for Grok Build.") @("Could not install") -Already "grok"

# Test-Interactive itself: automation that has a console says so with CI or NONINTERACTIVE.
Set-Item -Path function:script:Test-Interactive -Value $realTestInteractive
$saved = @{ CI = $env:CI; NONINTERACTIVE = $env:NONINTERACTIVE }
foreach ($variable in @("CI", "NONINTERACTIVE")) {
    $env:CI = $null
    $env:NONINTERACTIVE = $null
    Set-Item -Path "env:$variable" -Value "1"
    if (Test-Interactive) {
        Write-Output "[FAIL] Test-Interactive is true with $variable set"
        $failures += 1
    } else {
        Write-Output "[PASS] Test-Interactive is false with $variable set"
    }
}
$env:CI = $saved.CI
$env:NONINTERACTIVE = $saved.NONINTERACTIVE

Remove-Item -Path $work -Recurse -Force
if ($failures -gt 0) {
    Write-Output "$failures failed"
    exit 1
}
Write-Output "All Install.ps1 skills tests passed"
# The agent stubs leave $LASTEXITCODE set, and GitHub Actions' pwsh shell exits with it.
exit 0
