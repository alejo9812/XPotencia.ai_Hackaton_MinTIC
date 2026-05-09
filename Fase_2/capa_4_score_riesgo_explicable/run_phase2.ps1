$ErrorActionPreference = "Stop"

$phase2Root = $PSScriptRoot
$layer1Root = Resolve-Path (Join-Path $phase2Root "..\capa_1_ingesta_automatizada\pae_risk_tracker")
$layer2Root = Resolve-Path (Join-Path $phase2Root "..\capa_2_motor_reglas_cuantitativas\pae_risk_tracker")
$layer3Root = Resolve-Path (Join-Path $phase2Root "..\capa_3_analisis_semantico_llm\pae_risk_tracker")
$layer4Root = Resolve-Path (Join-Path $phase2Root "..\capa_4_score_riesgo_explicable\pae_risk_tracker")
$infoRoot = Join-Path $phase2Root "Info"
$trackerSrc = @(
  Join-Path $layer1Root "src"
  Join-Path $layer2Root "src"
  Join-Path $layer3Root "src"
  Join-Path $layer4Root "src"
) -join ";"
$trackerScripts = Join-Path $layer2Root "scripts"
$enrichedPath = Join-Path $layer1Root "data\processed\pae_contracts_enriched.parquet"

$env:PYTHONPATH = $trackerSrc

Write-Host "Fase_2 demo bootstrap"
Write-Host "Layer 1 root:  $layer1Root"
Write-Host "Layer 2 root:  $layer2Root"
Write-Host "Layer 3 root:  $layer3Root"
Write-Host "Layer 4 root:  $layer4Root"
Write-Host "Info root:    $infoRoot"

if (Test-Path $enrichedPath) {
  Write-Host "Regenerando outputs canónicos..."
  & py -3 (Join-Path $trackerScripts "score_contracts.py")
  if ($LASTEXITCODE -ne 0) {
    throw "No se pudo regenerar el ranking canónico."
  }
} else {
  Write-Host "No se encontro pae_contracts_enriched.parquet; se usaran los outputs existentes."
}

$apiProcess = Start-Process `
  -WindowStyle Hidden `
  -FilePath "py" `
  -ArgumentList @("-3", "-m", "uvicorn", "pae_risk_tracker.api.server:app", "--reload", "--port", "8000") `
  -WorkingDirectory $phase2Root `
  -PassThru

$dashboardProcess = Start-Process `
  -WindowStyle Hidden `
  -FilePath "node" `
  -ArgumentList @((Join-Path $infoRoot "server.mjs")) `
  -WorkingDirectory $infoRoot `
  -PassThru

Write-Host ""
Write-Host "API activa en http://127.0.0.1:8000"
Write-Host "Dashboard activo en http://localhost:4175"
Write-Host "Procesos: API=$($apiProcess.Id) Dashboard=$($dashboardProcess.Id)"
Write-Host "Ctrl+C cierra esta consola; para detener los procesos, usa Stop-Process con esos PID si hace falta."
