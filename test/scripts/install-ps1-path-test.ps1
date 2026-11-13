# Copyright 2026 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
#
# Tests how install/Install.ps1 adds the Spice CLI directory to the user's PATH, with no network
# and no change to the machine. It takes the script's own PATH statements (from the one that reads
# the user PATH to the one that writes it), swaps the two [Environment] calls for variables, and
# runs them against each starting PATH. The script's functions are loaded too, without running it.
#
# Usage: pwsh test/scripts/install-ps1-path-test.ps1

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

$getUserPath = '[Environment]::GetEnvironmentVariable("PATH", [EnvironmentVariableTarget]::User)'
$setUserPath = '\[Environment\]::SetEnvironmentVariable\("PATH",\s*(.+?),\s*\[EnvironmentVariableTarget\]::User\)'
$statements = @($ast.EndBlock.Statements | ForEach-Object { $_.Extent.Text })
$first = [Array]::FindIndex([string[]]$statements, [Predicate[string]] { param($text) $text.Contains($getUserPath) })
$last = [Array]::FindIndex([string[]]$statements, [Predicate[string]] { param($text) $text -match $setUserPath })
if ($first -lt 0 -or $last -lt $first) {
    throw "Install.ps1 no longer reads and then writes the user PATH where this test expects"
}
$block = ($statements[$first..$last] -join "`n").Replace($getUserPath, '$script:UserPath')
$block = [regex]::Replace($block, $setUserPath, '$script:Written = ($1)')
$updatePath = [ScriptBlock]::Create($block)

$spiceCliInstallDir = "C:\Users\me\.spice\bin"
$unchanged = "<not written>"

# Each case: the user PATH before, and the PATH the installer must write, or $unchanged when the
# directory is already there.
$cases = @(
    @{ Name = "unset user PATH"; Before = $null; After = "C:\Users\me\.spice\bin" },
    @{ Name = "empty user PATH"; Before = ""; After = "C:\Users\me\.spice\bin" },
    @{ Name = "a sibling directory only"; Before = "C:\Users\me\.spice\bin2;C:\Tools"; After = "C:\Users\me\.spice\bin2;C:\Tools;C:\Users\me\.spice\bin" },
    @{ Name = "already present"; Before = "C:\Tools;C:\Users\me\.spice\bin"; After = $unchanged },
    @{ Name = "present in another case, with a trailing backslash"; Before = "C:\Tools;c:\users\ME\.spice\bin\"; After = $unchanged },
    @{ Name = "a trailing separator"; Before = "C:\Tools;"; After = "C:\Tools;C:\Users\me\.spice\bin" },
    @{ Name = "empty entries and an unexpanded variable kept as they are"; Before = "%USERPROFILE%\go\bin;;C:\Tools"; After = "%USERPROFILE%\go\bin;;C:\Tools;C:\Users\me\.spice\bin" }
)

$failed = 0
foreach ($case in $cases) {
    $script:UserPath = $case.Before
    $script:Written = $unchanged
    try {
        & $updatePath | Out-Null
        $got = $script:Written
    } catch {
        $got = "threw: $($_.Exception.Message)"
    }
    if ($got -ceq $case.After) {
        Write-Host "ok   $($case.Name)"
    } else {
        Write-Host "FAIL $($case.Name): expected '$($case.After)', got '$got'"
        $failed += 1
    }
}

if ($failed -gt 0) {
    Write-Host "$failed of $($cases.Count) PATH cases failed"
    exit 1
}
Write-Host "All $($cases.Count) PATH cases passed"
