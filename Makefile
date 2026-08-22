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
up-l1: env ## Démarre le Level 1 (MinIO + Iceberg REST + Trino + générateur + Spark)
	# `--build` garantit que les images correspondent au code du dépôt : sans
	# lui, Compose réutilise une image existante et fait tourner une version
	# antérieure des jobs sans le signaler.
	$(COMPOSE) --profile l1 up -d --wait --build
	@set -a && . ./.env && set +a && printf '\n  Console MinIO : http://localhost:%s\n  UI Trino      : http://localhost:%s\n  Iceberg REST  : http://localhost:%s\n\n' \
		"$$MINIO_CONSOLE_PORT" "$$TRINO_PORT" "$$ICEBERG_REST_PORT"

.PHONY: down-l1
down-l1: ## Arrête le Level 1 (les données sont conservées)
	$(COMPOSE) --profile l1 down

.PHONY: ingest-l1
ingest-l1: ## Ingère la zone d'atterrissage vers les 8 tables Iceberg raw.*
	$(COMPOSE) exec spark python3 -m jobs.batch.ingest_raw

.PHONY: up-l2
up-l2: env ## Démarre le Level 2 (socle + Airflow)
	$(COMPOSE) --profile l2 up -d --wait --build
	@set -a && . ./.env && set +a && printf '\n  Airflow : http://localhost:%s (admin / %s)\n\n' \
		"$$AIRFLOW_PORT" "$$AIRFLOW_ADMIN_PASSWORD"

.PHONY: down-l2
down-l2: ## Arrête le Level 2 (les données sont conservées)
	$(COMPOSE) --profile l2 down

.PHONY: dags
dags: ## Liste les DAGs et signale les erreurs d'analyse
	$(COMPOSE) exec -T airflow-scheduler airflow dags list
	@$(COMPOSE) exec -T airflow-scheduler airflow dags list-import-errors

.PHONY: bronze-views
bronze-views: ## Crée les vues bronze.* (zone Bronze du médaillon) dans Trino
	$(COMPOSE) exec -T trino trino --no-progress -f /sql/internal/create_bronze_views.sql

.PHONY: silver-l2
silver-l2: ## Construit les tables silver.* depuis la couche brute
	$(COMPOSE) exec spark python3 -m jobs.batch.silver

.PHONY: gold-l2
gold-l2: ## Calcule les 7 tables de KPIs gold.* depuis la couche Silver
	$(COMPOSE) exec spark python3 -m jobs.batch.gold

.PHONY: medallion-l2
medallion-l2: bronze-views silver-l2 gold-l2 ## Enchaîne Bronze, Silver et Gold

.PHONY: compact-l1
compact-l1: ## Fusionne les petits fichiers Parquet des tables raw.*
	$(COMPOSE) exec spark python3 -m jobs.batch.compact

.PHONY: queries-l1
queries-l1: ## Exécute les requêtes analytiques du §1.4 contre Trino
	@bash scripts/queries_l1.sh

.PHONY: queries-l2
queries-l2: ## Exécute les requêtes analytiques du Level 2 contre Trino
	@bash scripts/queries_l1.sh level2

.PHONY: smoke-l1
smoke-l1: ## Vérifie de bout en bout générateur -> Spark -> Iceberg -> Trino
	@bash scripts/smoke_l1.sh

.PHONY: smoke-l2
smoke-l2: ## Vérifie le médaillon, les 7 KPIs, les DAGs et les seuils réglementaires
	@bash scripts/smoke_l2.sh

# --- Level 3 : streaming -------------------------------------------------------

.PHONY: up-l3
up-l3: env ## Démarre le Level 3 (socle + Airflow + Kafka + NiFi)
	$(COMPOSE) --profile l3 up -d --wait --build
	@echo ""
	@echo "  NiFi : https://localhost:$${NIFI_PORT:-8091}/nifi — certificat auto-signé"

.PHONY: down-l3
down-l3: ## Arrête le Level 3 (les données sont conservées)
	$(COMPOSE) --profile l3 down

# Le flux est construit depuis la machine hôte par l'API REST de NiFi ; le
# script n'utilise que la bibliothèque standard, il n'y a rien à installer.
.PHONY: nifi-flow
nifi-flow: ## (Re)construit et démarre le flux d'ingestion NiFi vers Kafka
	@python3 scripts/nifi_flow.py

.PHONY: nifi-status
nifi-status: ## État des processeurs NiFi et des files de contre-pression
	@python3 scripts/nifi_flow.py --status

.PHONY: nifi-replay
nifi-replay: ## Rejoue l'ingestion de tous les fichiers présents dans le bucket
	@python3 scripts/nifi_flow.py --reset-state

.PHONY: stream-silver
stream-silver: ## Job 1 streaming — raw-* vers silver-* et tables Iceberg (continu, Ctrl-C pour arrêter)
	$(COMPOSE) exec spark python3 -m jobs.streaming.raw_to_silver

.PHONY: stream-silver-once
stream-silver-once: ## Job 1 streaming — traite ce qui est disponible puis s'arrête
	$(COMPOSE) exec -T spark python3 -m jobs.streaming.raw_to_silver --once

.PHONY: stream-gold
stream-gold: ## Job 2 streaming — fraude, AML et liquidité depuis silver-* (continu)
	$(COMPOSE) exec spark python3 -m jobs.streaming.silver_to_gold

.PHONY: stream-gold-once
stream-gold-once: ## Job 2 streaming — traite ce qui est disponible puis s'arrête
	$(COMPOSE) exec -T spark python3 -m jobs.streaming.silver_to_gold --once

.PHONY: queries-l3
queries-l3: ## Requêtes du Level 3 : Lambda unifiée, alertes, rebut, fraîcheur
	@bash scripts/queries_l1.sh level3

.PHONY: smoke-l3
smoke-l3: ## Vérifie NiFi -> Kafka -> Spark -> Iceberg, la DLQ et la requête Lambda
	@bash scripts/smoke_l3.sh

.PHONY: topics
topics: ## Volumétrie des topics Kafka
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server kafka:9092 --topic-partitions '.*'

# --- Level 4 : visualisation et gouvernance ------------------------------------

.PHONY: up-l4
up-l4: env ## Démarre le Level 4 (toute la plateforme + Superset)
	$(COMPOSE) --profile l4 up -d --wait --build
	@echo ""
	@echo "  Superset : http://localhost:$${SUPERSET_PORT:-8088}"

.PHONY: down-l4
down-l4: ## Arrête le Level 4 (les données sont conservées)
	$(COMPOSE) --profile l4 down

# Les tableaux de bord sont construits par l'API depuis la machine hôte, comme
# le flux NiFi : le script n'utilise que la bibliothèque standard.
.PHONY: dashboards
dashboards: ## (Re)construit les 3 tableaux de bord Superset du §4.2
	@python3 scripts/superset_dashboards.py

# --- Level 4.1 : deploiement Kubernetes ----------------------------------------
#
# `--load-restrictor LoadRestrictionsNone` autorise Kustomize a lire des fichiers
# situes hors de sa racine. C'est deliberé : les descriptions de topics Trino,
# la configuration de Loki et les tableaux de bord Grafana vivent dans `docker/`
# et servent aux deux modes de deploiement. Les recopier sous `k8s/` creerait
# deux sources de verite destinees a diverger.
KUSTOMIZE := kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/overlays/local

.PHONY: k8s-render
k8s-render: ## Rend les manifestes Kubernetes sans les appliquer
	@$(KUSTOMIZE)

.PHONY: k8s-up
k8s-up: ## Deploie toute la plateforme sur le cluster courant
	@test -f k8s/base/secrets.env || 		(echo "Creer k8s/base/secrets.env depuis secrets.env.example" && false)
	@$(KUSTOMIZE) | kubectl apply -f -

.PHONY: k8s-status
k8s-status: ## Etat des pods de la plateforme, par namespace
	@kubectl get pods -A -l app.kubernetes.io/part-of=waba 		-o custom-columns='NAMESPACE:.metadata.namespace,POD:.metadata.name,ETAT:.status.phase,PRET:.status.containerStatuses[0].ready'

.PHONY: k8s-down
k8s-down: ## Supprime la plateforme du cluster, volumes compris
	@kubectl delete namespace waba-ingestion waba-processing waba-serving 		waba-governance waba-monitoring --ignore-not-found

# --- Demonstration --------------------------------------------------------------

.PHONY: demo-reset
demo-reset: ## Remet la plateforme dans un etat de demonstration connu
	@bash scripts/demo_reset.sh

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
