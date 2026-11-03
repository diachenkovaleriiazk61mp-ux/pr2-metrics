param([ValidateRange(1,4)][int]$Replicas=1)
$ErrorActionPreference='Stop'
Set-Location $PSScriptRoot
if (-not (Test-Path .env)) {
    $bytes=New-Object byte[] 24
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $value=([BitConverter]::ToString($bytes)).Replace('-','').ToLower()
    "POSTGRES_PASSWORD=$value`nPORT=18081`nPROMETHEUS_PORT=19090`nWEB_CPUS=1.5" | Set-Content .env -Encoding ascii
}
docker compose up -d --build --wait --scale "web=$Replicas"
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose failed. Check: docker compose logs web' }
docker compose restart lb
if ($LASTEXITCODE -ne 0) { throw 'Load balancer restart failed' }
Write-Host 'API: http://localhost:18081/healthz ; Prometheus: http://localhost:19090/targets'
