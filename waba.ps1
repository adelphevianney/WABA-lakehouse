<#
.SYNOPSIS
    WABA Group — point d'entrée du projet sous Windows (équivalent du Makefile).
.EXAMPLE
    .\waba.ps1 check
    .\waba.ps1 up-l1
    .\waba.ps1 smoke-l1
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'env', 'check', 'up-l1', 'down-l1', 'up-l2', 'down-l2', 'dags',
        'ingest-l1', 'compact-l1', 'bronze-views', 'silver-l2', 'gold-l2', 'queries-l1',
        'smoke-l1', 'ps', 'logs', 'sql', 'clean')]
    [string]$Command = 'help',

    [Parameter(Position = 1)]
    [string]$Service
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

$Compose = @('compose', '--env-file', '.env', '-f', 'docker/compose.yml')

function Invoke-Compose {
    param([string[]]$Arguments)
    # Docker écrit sa progression sur stderr sans que ce soit une erreur ; avec
    # `ErrorActionPreference = 'Stop'`, Windows PowerShell 5.1 la convertit en
    # exception et interrompt une commande qui a pourtant réussi. Seul le code
    # de sortie fait foi.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & docker @Compose @Arguments }
    finally { $ErrorActionPreference = $previous }
    if ($LASTEXITCODE -ne 0) { throw "docker compose a échoué (code $LASTEXITCODE)" }
}

function Get-EnvValue {
    param([Parameter(Mandatory)][string]$Key, [string]$Default = '')
    $m = Select-String -Path '.env' -Pattern "^\s*$Key\s*=\s*(.+?)\s*$" | Select-Object -First 1
    if ($m) { return $m.Matches[0].Groups[1].Value } else { return $Default }
}

function Initialize-Env {
    if (-not (Test-Path '.env')) {
        Copy-Item 'README.env.example' '.env'
        Write-Host '-> .env créé depuis README.env.example' -ForegroundColor Green
    }
    else {
        Write-Host '-> .env présent' -ForegroundColor DarkGray
    }
}

switch ($Command) {

    'help' {
        Write-Host ''
        Write-Host '  WABA Group — commandes disponibles' -ForegroundColor Cyan
        Write-Host ''
        Write-Host '    env        Crée .env à partir de README.env.example'
        Write-Host '    check      Vérifie les prérequis (Docker, Compose, mémoire)'
        Write-Host '    up-l1      Démarre le socle Level 1 (MinIO + Iceberg REST + Trino)'
        Write-Host '    down-l1    Arrête le Level 1 (données conservées)'
        Write-Host '    ingest-l1  Ingère la zone d''atterrissage vers les 8 tables raw.*'
        Write-Host '    compact-l1 Fusionne les petits fichiers Parquet des tables raw.*'
        Write-Host '    queries-l1 Exécute les requêtes analytiques du §1.4 contre Trino'
        Write-Host '    smoke-l1   Vérifie de bout en bout générateur -> Spark -> Iceberg -> Trino'
        Write-Host '    ps         Consommation mémoire des conteneurs'
        Write-Host '    logs <svc> Suit les logs d''un service'
        Write-Host '    sql        Shell SQL Trino interactif'
        Write-Host '    clean      Arrête tout et supprime les volumes (destructif)'
        Write-Host ''
    }

    'env' { Initialize-Env }

    'check' {
        docker version --format '  Docker Engine : {{.Server.Version}}'
        $v = docker compose version --short
        Write-Host "  Docker Compose: $v"
        $parts = (docker info --format '{{.MemTotal}} {{.NCPU}}') -split ' '
        Write-Host ("  RAM allouée   : {0:N1} Go / {1} vCPU" -f ($parts[0] / 1GB), $parts[1])
    }

    'up-l1' {
        Initialize-Env
        # `--build` garantit que les images correspondent au code du dépôt :
        # sans lui, Compose réutilise une image existante et fait tourner une
        # version antérieure des jobs sans le signaler.
        Invoke-Compose @('--profile', 'l1', 'up', '-d', '--wait', '--build')
        Write-Host ''
        Write-Host "  Console MinIO : http://localhost:$(Get-EnvValue 'MINIO_CONSOLE_PORT' '9001')" -ForegroundColor Green
        Write-Host "  UI Trino      : http://localhost:$(Get-EnvValue 'TRINO_PORT' '8080')" -ForegroundColor Green
        Write-Host "  Iceberg REST  : http://localhost:$(Get-EnvValue 'ICEBERG_REST_PORT' '8181')" -ForegroundColor Green
    }

    'down-l1' { Invoke-Compose @('--profile', 'l1', 'down') }

    'ingest-l1' { Invoke-Compose @('exec', 'spark', 'python3', '-m', 'jobs.batch.ingest_raw') }

    'up-l2' {
        Initialize-Env
        Invoke-Compose @('--profile', 'l2', 'up', '-d', '--wait', '--build')
        Write-Host ''
        Write-Host ("  Airflow : http://localhost:{0} (admin / {1})" -f `
            (Get-EnvValue 'AIRFLOW_PORT' '8090'), (Get-EnvValue 'AIRFLOW_ADMIN_PASSWORD' 'admin')) -ForegroundColor Green
    }

    'down-l2' { Invoke-Compose @('--profile', 'l2', 'down') }

    'dags' {
        Invoke-Compose @('exec', '-T', 'airflow-scheduler', 'airflow', 'dags', 'list')
        Invoke-Compose @('exec', '-T', 'airflow-scheduler', 'airflow', 'dags', 'list-import-errors')
    }

    'bronze-views' {
        Invoke-Compose @('exec', '-T', 'trino', 'trino', '--no-progress',
            '-f', '/sql/internal/create_bronze_views.sql')
    }

    'silver-l2' { Invoke-Compose @('exec', 'spark', 'python3', '-m', 'jobs.batch.silver') }

    'gold-l2' { Invoke-Compose @('exec', 'spark', 'python3', '-m', 'jobs.batch.gold') }

    'compact-l1' { Invoke-Compose @('exec', 'spark', 'python3', '-m', 'jobs.batch.compact') }

    'queries-l1' { & "$PSScriptRoot\scripts\queries_l1.ps1" }

    'smoke-l1' { & "$PSScriptRoot\scripts\smoke_l1.ps1" }

    'ps' { docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' }

    'logs' {
        if ($Service) { Invoke-Compose @('logs', '-f', $Service) }
        else { Invoke-Compose @('logs', '-f') }
    }

    'sql' { Invoke-Compose @('exec', 'trino', 'trino') }

    'clean' {
        $ok = Read-Host 'Supprimer tous les volumes (donnees MinIO + catalogue Iceberg) ? [y/N]'
        if ($ok -eq 'y') {
            Invoke-Compose @('--profile', 'l1', '--profile', 'l2', '--profile', 'l3', '--profile', 'l4', 'down', '-v')
        }
        else { Write-Host 'Annulé.' -ForegroundColor Yellow }
    }
}
