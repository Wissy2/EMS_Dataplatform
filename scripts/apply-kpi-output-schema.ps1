param(
    [string]$Container = "timescaledb",
    [string]$Database = "ems_db",
    [string]$User = "ems_user"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$schemaFile = Join-Path $repoRoot "database\init\05_kpi_outputs.sql"

if (-not (Test-Path -LiteralPath $schemaFile)) {
    throw "KPI schema file not found: $schemaFile"
}

Write-Host "Applying KPI output schema from $schemaFile to $Container/$Database..."
Get-Content -LiteralPath $schemaFile -Raw | docker exec -i $Container psql -U $User -d $Database -v ON_ERROR_STOP=1

Write-Host ""
Write-Host "KPI table shape after migration:"
docker exec -i $Container psql -U $User -d $Database -c "\d ems.equipment_kpis"
