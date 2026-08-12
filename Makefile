# =============================================================================
# WABA Group — Plateforme Analytique Financière Multi-Pays
# Point d'entrée unique du projet (Linux / macOS).
# Sous Windows, utiliser l'équivalent : .\waba.ps1 <commande>
# =============================================================================
SHELL   := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yml

.DEFAULT_GOAL := help

# --- Méta ---------------------------------------------------------------------

.PHONY: help
help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Crée le fichier .env à partir de README.env.example s'il n'existe pas
	@test -f .env || (cp README.env.example .env && echo "-> .env créé depuis README.env.example")
	@test -f .env && echo "-> .env présent"

.PHONY: check
check: ## Vérifie les prérequis (Docker, Compose, mémoire allouée)
	@docker version --format '  Docker Engine : {{.Server.Version}}'
	@docker compose version --short | sed 's/^/  Docker Compose: /'
	@docker info --format '{{.MemTotal}} {{.NCPU}}' \
		| awk '{printf "  RAM allouée   : %.1f Go / %s vCPU\n", $$1/1073741824, $$2}'

# --- Level 1 : socle lakehouse ------------------------------------------------

.PHONY: up-l1
up-l1: env ## Démarre le socle Level 1 (MinIO + Iceberg REST + Trino)
	$(COMPOSE) --profile l1 up -d --wait
	@set -a && . ./.env && set +a && printf '\n  Console MinIO : http://localhost:%s\n  UI Trino      : http://localhost:%s\n  Iceberg REST  : http://localhost:%s\n\n' \
		"$$MINIO_CONSOLE_PORT" "$$TRINO_PORT" "$$ICEBERG_REST_PORT"

.PHONY: down-l1
down-l1: ## Arrête le Level 1 (les données sont conservées)
	$(COMPOSE) --profile l1 down

.PHONY: smoke-l1
smoke-l1: ## Vérifie de bout en bout MinIO -> Iceberg REST -> Trino
	@bash scripts/smoke_l1.sh

# --- Exploitation --------------------------------------------------------------

.PHONY: ps
ps: ## Liste les conteneurs et leur consommation mémoire
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'

.PHONY: logs
logs: ## Suit les logs (make logs SVC=trino)
	$(COMPOSE) logs -f $(SVC)

.PHONY: sql
sql: ## Ouvre un shell SQL Trino interactif
	$(COMPOSE) exec trino trino

.PHONY: clean
clean: ## Arrête TOUT et supprime les volumes (destructif, demande confirmation)
	@read -p "Supprimer tous les volumes (données MinIO + catalogue Iceberg) ? [y/N] " ok; \
	 [ "$$ok" = "y" ] && $(COMPOSE) --profile l1 --profile l2 --profile l3 --profile l4 down -v || echo "Annulé."
