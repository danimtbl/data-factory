# Baixa os binarios do Hadoop necessarios para o Spark no Windows.
# Fonte: https://github.com/cdarlint/winutils (hadoop-3.3.5; o Spark 3.5.x
# embarca Hadoop 3.3.4 — winutils 3.3.5 e compatível).
#
# Uso: powershell -ExecutionPolicy Bypass -File scripts/setup_hadoop.ps1

$ErrorActionPreference = "Stop"

$target = Join-Path $PSScriptRoot "..\tools\hadoop\bin"
$baseUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"

New-Item -ItemType Directory -Force -Path $target | Out-Null

foreach ($file in @("winutils.exe", "hadoop.dll")) {
    $dest = Join-Path $target $file
    if (Test-Path $dest) {
        Write-Host "ja existe: $dest"
        continue
    }
    Write-Host "baixando $file ..."
    curl.exe -L -o $dest "$baseUrl/$file" --retry 3
}

Write-Host "pronto. HADOOP_HOME deve apontar para: $((Resolve-Path (Join-Path $target '..')).Path)"
