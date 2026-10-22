
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

$envFile = Join-Path $ProjectRoot ".env"

if (-not (Test-Path $envFile)) {
    throw ".env file not found."
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()

    if (-not $line -or $line.StartsWith("#")) {
        return
    }

    if ($line -match '^([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        $name = $Matches[1]
        $value = $Matches[2].Trim()

        if (
            $value.Length -ge 2 -and
            (
                ($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))
            )
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        [Environment]::SetEnvironmentVariable(
            $name, $value, "Process"
        )
    }
}

$required = @(
    "DATABASE_URL",
    "POSTGRES_PASSWORD",
    "REDIS_PASSWORD",
    "REDIS_ADDR",
    "WEBHOOK_ENCRYPTION_KEY",
    "SUBSCRIPTION_API_TOKEN",
    "EVENT_INGESTION_API_TOKEN"
)

foreach ($name in $required) {
    if ([string]::IsNullOrWhiteSpace(
        [Environment]::GetEnvironmentVariable($name, "Process")
    )) {
        throw "Required configuration is missing: $name"
    }
}

Write-Host "EventFlow environment loaded successfully."
Write-Host "Required variables are present; secret values are hidden."
