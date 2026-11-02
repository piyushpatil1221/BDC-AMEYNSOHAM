# One-command live demo on Windows (PowerShell):  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
# Starts the full stack (core + monitoring), submits the Flink job, replays MIND at 60x, injects a
# breaking-news burst after a few minutes, keeps light /recommend traffic flowing, and serves the dashboard.
param(
    [int]$ReplayMinutes = 2880,     # event-time minutes to replay (2880 = the full 2-day subset, ~48 min wall)
    [int]$BurstAfterSeconds = 240,  # when to inject the sports burst
    [switch]$Fresh                  # wipe all state first
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:Path += ";$env:ProgramFiles\Docker\Docker\resources\bin"

function Wait-Until([scriptblock]$Check, [int]$TimeoutSec, [string]$What) {
    $end = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $end) { try { if (& $Check) { return } } catch {} ; Start-Sleep 3 }
    throw "Timed out waiting for $What"
}

if (-not (Test-Path "data\subset\behaviors.tsv")) {
    Write-Host "Preparing data (first run only)..."
    docker compose --profile core --profile tools run --rm --no-deps replay replay.download
    docker compose --profile core --profile tools run --rm --no-deps replay replay.subset
}
if ($Fresh) { docker compose --profile core --profile monitoring --profile tools --profile submit down -v }

Write-Host "Starting core + monitoring stack..."
docker compose --profile core --profile monitoring up -d   # builds only images that are missing
Wait-Until { (docker inspect -f "{{.State.Status}}" sohamameybdc-init-1) -eq "exited" } 300 "bootstrap"
Wait-Until { (Invoke-RestMethod http://localhost:8081/overview -TimeoutSec 3).'slots-total' -ge 2 } 180 "Flink"

$running = (Invoke-RestMethod http://localhost:8081/jobs/overview).jobs | Where-Object state -eq RUNNING
if (-not $running) {
    Write-Host "Submitting the Flink job..."
    docker compose --profile core --profile submit run --rm --no-deps flink-submit
}

Write-Host "Starting the dashboard on http://localhost:3000 ..."
Push-Location dashboard
if (-not (Test-Path "node_modules")) { npm install --no-audit --no-fund }
if (-not (Test-Path ".next\BUILD_ID")) { node node_modules/next/dist/bin/next build }
Start-Process -WindowStyle Minimized -FilePath "node" -ArgumentList "node_modules/next/dist/bin/next", "start", "-p", "3000"
Pop-Location

Write-Host "Replaying $ReplayMinutes event-minutes at 60x (background)..."
Start-Process -WindowStyle Minimized -FilePath "docker" -ArgumentList @("compose", "--profile", "core", "--profile", "tools", "run", "--rm", "--no-deps", "replay", "replay.producer", "--max-minutes", "$ReplayMinutes")
Start-Sleep 20
Start-Process "http://localhost:3000"

Start-Sleep $BurstAfterSeconds
Write-Host "Injecting the breaking-news burst (sports)..."
docker compose --profile core --profile tools run --rm --no-deps replay replay.burst --category sports

Write-Host "Generating steady recommendation traffic for latency metrics (10 min)..."
docker compose --profile core --profile tools run --rm --no-deps replay replay.loadgen --rps 10 --seconds 600
Write-Host "Demo running. Dashboard: http://localhost:3000  Grafana: http://localhost:3001/d/newsrec  Flink: http://localhost:8081"
