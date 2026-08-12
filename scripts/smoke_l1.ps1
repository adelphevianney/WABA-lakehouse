<#
.SYNOPSIS
    Test de fumée du socle Level 1 (équivalent Windows de scripts/smoke_l1.sh).

.DESCRIPTION
    Prouve la chaîne complète MinIO -> catalogue Iceberg REST -> Trino :
      1. les trois buckets du sujet existent ;
      2. Trino expose le catalogue `iceberg` ;
      3. une table Iceberg partitionnée par country_code peut être créée,
         alimentée et relue en SQL ;
      4. les fichiers de données atterrissent bien dans MinIO.

.NOTES
    Ce fichier doit rester encodé en UTF-8 AVEC BOM : Windows PowerShell 5.1
    interprète sinon les accents en ANSI, ce qui casse l'analyse du script.
#>
Set-Location -Path (Split-Path -Parent $PSScriptRoot)

# Les commandes natives (docker, trino) écrivent des avertissements sur stderr
# sans que ce soit une erreur : on ne veut pas qu'ils interrompent le script.
$ErrorActionPreference = 'Continue'

$Compose = @('compose', '--env-file', '.env', '-f', 'docker/compose.yml')

function Write-Step { param($m) Write-Host "`n==> $m" -ForegroundColor Cyan }
function Write-Ok { param($m) Write-Host "    OK  $m" -ForegroundColor Green }
function Stop-Smoke { param($m) Write-Host "    KO  $m" -ForegroundColor Red; exit 1 }

function Invoke-TrinoSql {
    param(
        [Parameter(Mandatory)][string]$Sql,
        [ValidateSet('TSV', 'ALIGNED')][string]$Format = 'TSV'
    )
    $out = & docker @Compose exec -T trino trino --no-progress --output-format $Format --execute $Sql 2>$null
    if ($LASTEXITCODE -ne 0) {
        # Relance sans filtrage pour rendre l'erreur Trino visible.
        & docker @Compose exec -T trino trino --no-progress --execute $Sql
        Stop-Smoke "échec de la requête : $Sql"
    }
    return $out
}

function Invoke-Mc {
    param([Parameter(Mandatory)][string]$McCommand)
    # Les $VARIABLES restent littérales : elles sont résolues par le shell du
    # conteneur mc, jamais par PowerShell ni écrites dans le dépôt.
    $inner = 'mc alias set waba http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" > /dev/null && ' + $McCommand
    $out = & docker @Compose run --rm --no-deps minio-init $inner 2>$null
    if ($LASTEXITCODE -ne 0) { Stop-Smoke "échec de la commande mc : $McCommand" }
    return $out
}

# --- 1. Buckets MinIO --------------------------------------------------------
Write-Step '1/4 — Buckets MinIO'
foreach ($bucket in @('raw-landing', 'lakehouse', 'archive')) {
    $null = & docker @Compose exec -T minio test -d "/data/$bucket" 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Ok "bucket $bucket" } else { Stop-Smoke "bucket $bucket absent" }
}

# --- 2. Catalogue Trino ------------------------------------------------------
Write-Step '2/4 — Catalogue Iceberg visible depuis Trino'
if ((Invoke-TrinoSql -Sql 'SHOW CATALOGS') -contains 'iceberg') { Write-Ok 'catalogue iceberg exposé' }
else { Stop-Smoke 'catalogue iceberg absent' }

# --- 3. Aller-retour SQL sur une table Iceberg -------------------------------
Write-Step "3/4 — Création, écriture et lecture d'une table Iceberg partitionnée"
$null = Invoke-TrinoSql -Sql 'DROP TABLE IF EXISTS iceberg.smoke.ping'
$null = Invoke-TrinoSql -Sql 'CREATE SCHEMA IF NOT EXISTS iceberg.smoke'
$null = Invoke-TrinoSql -Sql @"
CREATE TABLE iceberg.smoke.ping (
    country_code varchar,
    entity_type  varchar,
    amount       double,
    event_date   date
) WITH (partitioning = ARRAY['country_code'])
"@
Write-Ok 'table créée et partitionnée par country_code'

$null = Invoke-TrinoSql -Sql @"
INSERT INTO iceberg.smoke.ping VALUES
    ('CI','BANK',         125000.0, DATE '2026-01-15'),
    ('SN','INSURANCE',     48000.0, DATE '2026-01-15'),
    ('GH','MOBILE_MONEY',    900.0, DATE '2026-01-16')
"@
Write-Ok '3 lignes insérées'

Write-Host ''
Invoke-TrinoSql -Format ALIGNED -Sql @"
SELECT country_code, count(*) AS n, sum(amount) AS total_local
FROM iceberg.smoke.ping GROUP BY 1 ORDER BY 1
"@

$count = (Invoke-TrinoSql -Sql 'SELECT count(*) FROM iceberg.smoke.ping') -replace '[^0-9]', ''
if ($count -eq '3') { Write-Ok 'relecture cohérente (3 lignes)' }
else { Stop-Smoke "attendu 3 lignes, obtenu '$count'" }

# --- 4. Persistance réelle dans MinIO ----------------------------------------
# La vérification passe par l'API S3 (mc) et non par le système de fichiers :
# c'est la vue qu'ont réellement Spark et Trino sur le stockage.
Write-Step "4/4 — Objets Iceberg présents dans le bucket lakehouse (API S3)"
$objects = Invoke-Mc 'mc ls --recursive waba/lakehouse'

if (-not ($objects -match '\.parquet')) { Stop-Smoke 'aucun objet Parquet dans le bucket lakehouse' }
($objects | Where-Object { $_ -match '\.parquet' } | Select-Object -First 3) |
    ForEach-Object { Write-Host "    $(($_ -split '\s+')[-1])" -ForegroundColor DarkGray }
Write-Ok 'données Parquet écrites dans s3://lakehouse'

# Iceberg matérialise la clé de partition dans le chemin des objets.
if ($objects -match 'country_code=') { Write-Ok 'partitionnement physique par country_code confirmé' }
else { Stop-Smoke 'les objets ne sont pas partitionnés par country_code' }

if ($objects -match '\.metadata\.json') { Write-Ok 'métadonnées Iceberg (snapshots) écrites' }
else { Stop-Smoke 'aucune métadonnée Iceberg trouvée' }

# --- Nettoyage ---------------------------------------------------------------
$null = Invoke-TrinoSql -Sql 'DROP TABLE iceberg.smoke.ping'
$null = Invoke-TrinoSql -Sql 'DROP SCHEMA iceberg.smoke'

Write-Host "`n*** Socle Level 1 opérationnel ***`n" -ForegroundColor Green
