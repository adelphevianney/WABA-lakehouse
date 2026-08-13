<#
.SYNOPSIS
    Requêtes analytiques du Level 1 (équivalent Windows de scripts/queries_l1.sh).

.DESCRIPTION
    Exécute contre Trino les requêtes de sql/level1/ : soldes par pays, volumes
    de transactions, comptages par entité, et traçabilité de l'ingestion.

    Les fichiers sont montés dans le conteneur Trino et exécutés avec -f plutôt
    que passés en ligne de commande : le SQL ne traverse aucun shell, donc aucun
    échappement ne peut l'altérer.

.NOTES
    Ce fichier doit rester encodé en UTF-8 AVEC BOM : Windows PowerShell 5.1
    interprète sinon les accents en ANSI, ce qui casse l'analyse du script.
#>
Set-Location -Path (Split-Path -Parent $PSScriptRoot)
$ErrorActionPreference = 'Continue'

$Compose = @('compose', '--env-file', '.env', '-f', 'docker/compose.yml')
$files = Get-ChildItem -Path 'sql/level1' -Filter '*.sql' | Sort-Object Name

foreach ($file in $files) {
    # La première ligne de commentaire de chaque fichier sert de titre. La
    # lecture doit être explicitement en UTF-8 : Windows PowerShell 5.1
    # interprète sinon les accents en ANSI.
    $title = (Get-Content $file.FullName -First 1 -Encoding UTF8) -replace '^--\s*', ''
    Write-Host "`n=== $title`n" -ForegroundColor Cyan

    # `-f` produit du CSV par défaut, là où `--execute` produit un tableau
    # aligné : le format est demandé explicitement pour rester lisible.
    & docker @Compose exec -T trino trino --no-progress --output-format ALIGNED `
        -f "/sql/level1/$($file.Name)" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Échec de $($file.Name)" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n$($files.Count) requêtes analytiques exécutées`n" -ForegroundColor Green
