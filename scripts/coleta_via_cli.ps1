# Plano B da coleta: baixa os arquivos do Inside Airbnb na maquina local e envia ao Volume
# workspace.bronze.raw_files usando o Databricks CLI (ja autenticado com `databricks auth login`).
# Mantem a mesma estrutura <cidade>/<snapshot>/<arquivo> usada pelo notebook 01_bronze/01_coleta_raw.
#
# Uso (PowerShell):  .\scripts\coleta_via_cli.ps1

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # acelera o Invoke-WebRequest

$volume = "dbfs:/Volumes/workspace/bronze/raw_files"
$fontes = @(
    @{ cidade = "sp"; snapshot = "2026-06-14"; base = "https://data.insideairbnb.com/brazil/sp/s%C3%A3o-paulo/2026-06-14" },
    @{ cidade = "rj"; snapshot = "2026-06-24"; base = "https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2026-06-24" }
)
$arquivos = @("data/listings.csv.gz", "data/calendar.csv.gz", "data/reviews.csv.gz", "visualisations/neighbourhoods.csv")
$pastaTemp = Join-Path $env:TEMP "insideairbnb"

foreach ($f in $fontes) {
    $pastaLocal = Join-Path $pastaTemp "$($f.cidade)\$($f.snapshot)"
    New-Item -ItemType Directory -Force -Path $pastaLocal | Out-Null
    $pastaRemota = "$volume/$($f.cidade)/$($f.snapshot)"
    databricks fs mkdir $pastaRemota

    foreach ($a in $arquivos) {
        $nome = Split-Path $a -Leaf
        $local = Join-Path $pastaLocal $nome
        Write-Host "Baixando $($f.cidade)/$nome ..."
        Invoke-WebRequest -Uri "$($f.base)/$a" -OutFile $local
        Write-Host "Enviando para $pastaRemota/$nome ..."
        databricks fs cp $local "$pastaRemota/$nome" --overwrite
    }
}

databricks fs ls $volume/sp
databricks fs ls $volume/rj
