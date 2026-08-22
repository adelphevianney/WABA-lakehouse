#!/usr/bin/env bash
# =============================================================================
# Remet la plateforme dans un état de démonstration connu.
#
# Une démonstration filmée ne supporte pas l'imprévu : un topic à moitié plein,
# un point de reprise qui saute une étape, un flux NiFi arrêté. Ce script ramène
# le chemin temps réel à un état déterministe, puis dépose un jeu de données
# frais sur les huit pays.
#
# Il ne touche ni au lakehouse ni au catalogue : l'historique consolidé fait
# partie de ce qu'on montre.
#
# Prérequis : profil l4 démarré (`make up-l4`).
#
# Usage : bash scripts/demo_reset.sh    (ou `make demo-reset`)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

COMPOSE=(docker compose --env-file .env -f docker/compose.yml)

etape() { printf '\n\033[36m==> %s\033[0m\n' "$1" >&2; }
ok()    { printf '\033[32m    %s\033[0m\n' "$1" >&2; }

TOPICS=(raw-bank-transactions raw-insurance-operations raw-mobile-money-payments
        raw-loan-repayments silver-bank-transactions silver-insurance-operations
        silver-mobile-money gold-fraud-alerts gold-aml-events gold-liquidity-alerts
        dlq-financial-events)

kafka_topics() { "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
                   --bootstrap-server kafka:9092 "$@" 2>/dev/null; }

# --- 1. Bus de messages -------------------------------------------------------
etape "1/5 — Topics Kafka remis à zéro"
for topic in "${TOPICS[@]}"; do
  kafka_topics --delete --topic "$topic" >/dev/null || true
done

# La suppression est asynchrone : Kafka marque le topic, puis le retire.
# Recréer avant que le retrait ne soit effectif échoue. On attend donc la
# disparition réelle plutôt qu'un délai fixe, tantôt trop court, tantôt du
# temps perdu.
for _ in $(seq 1 30); do
  restants=$(kafka_topics --list | grep -cE '^(raw|silver|gold|dlq)-' || true)
  [ "${restants:-0}" = "0" ] && break
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  kafka_topics --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1 >/dev/null
done
ok "${#TOPICS[@]} topics recréés, vides"

# --- 2. Points de reprise -----------------------------------------------------
# Sans cet effacement, les jobs reprendraient à des offsets de topics qui
# n'existent plus et ne traiteraient rien : la démonstration montrerait un flux
# qui ne coule pas.
etape "2/5 — Points de reprise des jobs de streaming"
"${COMPOSE[@]}" exec -T spark rm -rf /checkpoints/raw_to_silver /checkpoints/silver_to_gold
ok "points de reprise effacés"

# --- 3. Tables d'alertes ------------------------------------------------------
etape "3/5 — Tables d'alertes Gold"
"${COMPOSE[@]}" exec -T spark python3 -c "
from jobs.batch.session import build_session
spark = build_session('demo-reset')
for table in ('fraud_alerts', 'aml_events', 'liquidity_alerts'):
    spark.sql('DROP TABLE IF EXISTS iceberg.gold.%s' % table)
spark.stop()" >/dev/null 2>&1
ok "alertes remises à zéro — elles se reconstruiront devant la caméra"

# --- 4. Flux NiFi -------------------------------------------------------------
# La reconstruction remet aussi à zéro l'état du recensement : les fichiers
# déposés ensuite seront tous vus, sans dépendre de ce qui a précédé.
etape "4/5 — Flux NiFi"
python scripts/nifi_flow.py >/dev/null
ok "9 processeurs en marche, recensement remis à zéro"

# --- 5. Données de démonstration ---------------------------------------------
etape "5/5 — Jeu de données sur les huit pays"
"${COMPOSE[@]}" exec -T streamlit python -m generator.seed \
  --reuse-referentials --days 1 --rows 600 2>&1 | grep -E "fichiers," | sed 's/^.*— /    /' || true
ok "déposé — NiFi le recensera dans les 30 secondes"

printf '\n\033[32m*** Plateforme prête pour la démonstration ***\033[0m\n'
printf '    Superset  http://localhost:8088\n'
printf '    Grafana   http://localhost:3000\n'
printf '    Airflow   http://localhost:8090\n'
printf '    NiFi      https://localhost:8091/nifi\n'
printf '    MinIO     http://localhost:9001\n\n'
printf '    Plan de tournage : writeup/demonstration.md\n\n'
