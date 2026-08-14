<#
.SYNOPSIS
    Test de fumée du socle Level 1 (équivalent Windows de scripts/smoke_l1.sh).

.DESCRIPTION
    Prouve la chaîne complète MinIO -> catalogue Iceberg REST -> Trino :
      1. les trois buckets du sujet existent ;
      2. Trino expose le catalogue `iceberg` ;
      3. une table Iceberg partitionnée par country_code peut être créée,
         alimentée et relue en SQL ;
      4. les fichiers atterrissent bien dans MinIO ;
      5. le générateur produit un jeu de données sur les 8 pays ;
      6. ces données satisfont les critères du Level 1 ;
      7. Spark les ingère dans les 8 tables Iceberg raw.* ;
      8. rejouer l'ingestion ne crée aucun doublon ;
      9. les requêtes analytiques du §1.4 s'exécutent contre Trino.

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
Write-Step '1/9 — Buckets MinIO'
foreach ($bucket in @('raw-landing', 'lakehouse', 'archive')) {
    $null = & docker @Compose exec -T minio test -d "/data/$bucket" 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Ok "bucket $bucket" } else { Stop-Smoke "bucket $bucket absent" }
}

# --- 2. Catalogue Trino ------------------------------------------------------
Write-Step '2/9 — Catalogue Iceberg visible depuis Trino'
if ((Invoke-TrinoSql -Sql 'SHOW CATALOGS') -contains 'iceberg') { Write-Ok 'catalogue iceberg exposé' }
else { Stop-Smoke 'catalogue iceberg absent' }

# --- 3. Aller-retour SQL sur une table Iceberg -------------------------------
Write-Step "3/9 — Création, écriture et lecture d'une table Iceberg partitionnée"
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
Write-Step "4/9 — Objets Iceberg présents dans le bucket lakehouse (API S3)"
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

# --- Nettoyage de la table de contrôle ---------------------------------------
$null = Invoke-TrinoSql -Sql 'DROP TABLE iceberg.smoke.ping'
$null = Invoke-TrinoSql -Sql 'DROP SCHEMA iceberg.smoke'

# --- 5. Générateur -----------------------------------------------------------
Write-Step "5/9 — Génération et dépôt d'un jeu de données multi-pays"
# `--reuse-referentials` est essentiel en exécution répétée : régénérer les
# référentiels rendrait orphelines les clés des fichiers déjà déposés.
$seeded = & docker @Compose exec -T streamlit python -m generator.seed `
    --preset demo --reuse-referentials --seed 42 2>&1
if ($LASTEXITCODE -ne 0) { Stop-Smoke "échec de la génération : $($seeded | Select-Object -Last 1)" }
Write-Host "    $($seeded | Select-Object -Last 1)" -ForegroundColor DarkGray
Write-Ok 'jeu de données déposé dans raw-landing'

# --- 6. Conformité des données -----------------------------------------------
Write-Step '6/9 — Conformité des données déposées'
& docker @Compose exec -T streamlit python -m generator.verify
if ($LASTEXITCODE -ne 0) { Stop-Smoke 'des contrôles de conformité ont échoué' }

# --- 7. Ingestion Spark vers les tables Iceberg -------------------------------
Write-Step '7/9 — Ingestion Spark vers les 8 tables raw.*'
$ingested = & docker @Compose exec -T spark python3 -m jobs.batch.ingest_raw 2>$null
if ($LASTEXITCODE -ne 0) { Stop-Smoke "échec de l'ingestion Spark" }
($ingested | Select-Object -Last 12) | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }

$tables = Invoke-TrinoSql -Sql 'SHOW TABLES FROM iceberg.raw'
foreach ($table in @('customers', 'accounts', 'branches', 'products',
                     'bank_transactions', 'insurance_operations',
                     'mobile_money_payments', 'loan_repayments')) {
    if ($tables -notcontains $table) { Stop-Smoke "table raw.$table absente" }
}
Write-Ok 'les 8 tables raw.* existent dans Trino'

# Somme des lignes des huit tables, servant de témoin à la vérification
# d'idempotence de l'étape suivante.
$RawTotalSql = @"
SELECT (SELECT count(*) FROM iceberg.raw.customers)
     + (SELECT count(*) FROM iceberg.raw.accounts)
     + (SELECT count(*) FROM iceberg.raw.branches)
     + (SELECT count(*) FROM iceberg.raw.products)
     + (SELECT count(*) FROM iceberg.raw.bank_transactions)
     + (SELECT count(*) FROM iceberg.raw.insurance_operations)
     + (SELECT count(*) FROM iceberg.raw.mobile_money_payments)
     + (SELECT count(*) FROM iceberg.raw.loan_repayments)
"@

$before = (Invoke-TrinoSql -Sql $RawTotalSql) -replace '[^0-9]', ''
if ([int]$before -le 0) { Stop-Smoke 'les tables raw.* sont vides' }
Write-Ok "$before lignes ingérées, interrogeables en SQL"

# Garde-fou contre le morcellement. Un fichier par jour et par pays donne des
# partitions bien remplies ; si la génération repassait à un fichier couvrant
# toute la période, les partitions retomberaient à quelques dizaines de lignes
# et le stockage par ligne serait multiplié par dix.
# La requête passe par un fichier : les guillemets exigés par les tables de
# métadonnées Iceberg ne survivent pas au passage en ligne de commande.
$partitionRows = (& docker @Compose exec -T trino trino --no-progress --output-format TSV `
        -f /sql/internal/partition_health.sql 2>$null) -replace '[^0-9]', ''
if ([int]$partitionRows -ge 100) {
    Write-Ok "partitions saines ($partitionRows lignes par partition en moyenne)"
}
else { Stop-Smoke "partitionnement trop fin : $partitionRows lignes par partition" }

# --- 8. Idempotence -----------------------------------------------------------
# Critère explicite de l'énoncé. La graine étant identique, le générateur
# reproduit exactement les mêmes identifiants : les fichiers redéposés portent
# un nouveau numéro de séquence mais un contenu ligne à ligne identique.
Write-Step '8/9 — Idempotence : réingestion des mêmes données'
$reseeded = & docker @Compose exec -T streamlit python -m generator.seed `
    --preset demo --reuse-referentials --seed 42 2>&1
Write-Host "    $($reseeded | Select-Object -Last 1)" -ForegroundColor DarkGray

$replayed = & docker @Compose exec -T spark python3 -m jobs.batch.ingest_raw 2>$null
if ($LASTEXITCODE -ne 0) { Stop-Smoke "échec de la réingestion" }
($replayed | Select-Object -Last 12) | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }

$after = (Invoke-TrinoSql -Sql $RawTotalSql) -replace '[^0-9]', ''
if ($after -eq $before) { Write-Ok "aucun doublon créé ($before lignes avant et après)" }
else { Stop-Smoke "idempotence rompue : $before lignes avant, $after après" }

# --- 9. Requêtes analytiques --------------------------------------------------
# Dernier point du §1.4 : « permettre des requêtes analytiques de base ».
# Les résultats détaillés s'obtiennent avec `.\waba.ps1 queries-l1` ; ici on
# vérifie seulement qu'elles s'exécutent toutes sans erreur.
Write-Step '9/9 — Requêtes analytiques du §1.4'
foreach ($query in Get-ChildItem -Path 'sql/level1' -Filter '*.sql' | Sort-Object Name) {
    $null = & docker @Compose exec -T trino trino --no-progress -f "/sql/level1/$($query.Name)" 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Ok $query.Name }
    else { Stop-Smoke "la requête $($query.Name) a échoué" }
}

Write-Host "`n*** Socle Level 1 opérationnel ***`n" -ForegroundColor Green
