#!/usr/bin/env bash
# =============================================================================
# Test de fumée du Level 3 — pipeline hybride batch et streaming.
#
# Vérifie chaque critère de la grille d'évaluation du niveau :
#   1. Kafka expose les 11 topics du §3.2 ;
#   2. le flux NiFi est construit et démarré, contre-pression comprise ;
#   3. un dépôt de fichier atteint les topics raw-* — NiFi vers Kafka ;
#   4. le Job 1 alimente les topics silver-* ET les tables Iceberg silver.* ;
#   5. les messages malformés atterrissent dans dlq-financial-events avec un motif ;
#   6. le Job 2 retrouve les anomalies injectées, comparé à l'oracle du générateur ;
#   7. l'AML publie les franchissements du seuil déclaratif ;
#   8. la requête Lambda unifiée s'exécute et ne compte pas deux fois ;
#   9. le batch et le streaming cohabitent sur la même zone d'atterrissage ;
#  10. rejouer les topics ne duplique rien.
#
# Prérequis : profil l3 démarré (`make up-l3`) et Levels 1-2 exécutés.
#
# Usage : bash scripts/smoke_l3.sh    (ou `make smoke-l3`)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Sous Git Bash, MSYS réécrit les chemins absolus passés aux conteneurs.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

COMPOSE=(docker compose --env-file .env -f docker/compose.yml)

step() { printf '\n\033[36m==> %s\033[0m\n' "$1" >&2; }
ok()   { printf '\033[32m    OK  %s\033[0m\n' "$1" >&2; }
fail() { printf '\033[31m    KO  %s\033[0m\n' "$1" >&2; exit 1; }

contains() { [[ "$2" == *"$1"* ]]; }

trino_sql() {
  "${COMPOSE[@]}" exec -T trino trino --no-progress --output-format TSV --execute "$1" 2>/dev/null
}

kafka() { "${COMPOSE[@]}" exec -T kafka "$@" 2>/dev/null; }

# Somme des offsets d'un topic. Les marqueurs de transaction du producteur NiFi
# en occupent un par partition : le compte est un majorant du nombre de messages,
# ce qui suffit pour constater qu'un flux progresse.
offsets() {
  kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$1" \
    | awk -F: '{s+=$3} END {print s+0}'
}

# Attend qu'une commande renvoie une valeur supérieure à un seuil, ou abandonne.
attendre() {
  local description="$1" seuil="$2" limite="$3"; shift 3
  local i valeur
  for ((i = 0; i < limite; i++)); do
    valeur=$("$@")
    [ "${valeur:-0}" -gt "$seuil" ] && { echo "$valeur"; return 0; }
    sleep 5
  done
  fail "${description} : toujours à ${valeur:-0} après $((limite * 5)) s"
}

# Le Burkina Faso est le seul pays où les quatre entités du groupe opèrent :
# banque, assurance, mobile money et microfinance. Un dépôt y produit donc de
# quoi éprouver les trois règles de fraude et la surveillance AML, là où un pays
# sans mobile money laisserait la règle 2 sans matière.
PAYS_TEST="${WABA_SMOKE_COUNTRY:-BF}"

# --- 1. Les topics du bus -----------------------------------------------------
step "1/10 — Les 11 topics du §3.2"
topics=$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list)
for topic in raw-bank-transactions raw-insurance-operations raw-mobile-money-payments \
             raw-loan-repayments silver-bank-transactions silver-insurance-operations \
             silver-mobile-money gold-fraud-alerts gold-aml-events gold-liquidity-alerts \
             dlq-financial-events; do
  contains "$topic" "$topics" || fail "topic ${topic} absent"
done
ok "les 11 topics existent"

# --- 2. Le flux NiFi ----------------------------------------------------------
step "2/10 — Flux NiFi d'ingestion"
# Le flux n'est construit que s'il n'existe pas encore. Le reconstruire remet à
# zéro l'état du recensement de NiFi, et republierait l'intégralité de la zone
# d'atterrissage : sans conséquence sur les tables, grâce au MERGE, mais le test
# durerait plus longtemps à chaque exécution.
if ! etat=$(python scripts/nifi_flow.py --status 2>/dev/null); then
  python scripts/nifi_flow.py >/dev/null || fail "construction du flux NiFi impossible"
  etat=$(python scripts/nifi_flow.py --status)
fi
arretes=$(echo "$etat" | grep -c "Stopped" || true)
[ "$arretes" = "0" ] || fail "${arretes} processeur(s) à l'arrêt"
ok "9 processeurs en marche"

# La contre-pression est un critère explicite : les seuils doivent être calibrés
# en deçà des valeurs par défaut de NiFi (10 000 objets, 1 Go).
contains "seuil 500 / 64 MB" "$etat" \
  && ok "contre-pression calibrée devant le producteur Kafka" \
  || fail "la file d'entrée du producteur garde les seuils par défaut"

# --- 3. NiFi vers Kafka -------------------------------------------------------
step "3/10 — Un fichier déposé atteint les topics raw-*"
avant_raw=$(offsets raw-bank-transactions)
"${COMPOSE[@]}" exec -T streamlit python -m generator.seed --reuse-referentials \
  --countries "$PAYS_TEST" --days 1 --rows 300 >/dev/null 2>&1 \
  || fail "génération impossible"
ok "fichiers déposés pour ${PAYS_TEST}"

apres_raw=$(attendre "NiFi n'a rien publié" "$avant_raw" 24 offsets raw-bank-transactions)
ok "raw-bank-transactions : ${avant_raw} -> ${apres_raw} offsets"

# --- 4. Job 1 : double récepteur ----------------------------------------------
step "4/10 — Job 1 alimente les topics silver-* et les tables Iceberg"
avant_silver_table=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
avant_silver_topic=$(offsets silver-bank-transactions)

"${COMPOSE[@]}" exec -T spark python3 -m jobs.streaming.raw_to_silver --once 2>&1 \
  | grep -E "lot [0-9]+ —" | sed 's/^.*— /    /' || fail "le Job 1 a échoué"

apres_silver_table=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
apres_silver_topic=$(offsets silver-bank-transactions)

[ "${apres_silver_table:-0}" -gt "${avant_silver_table:-0}" ] \
  && ok "table silver.bank_transactions : ${avant_silver_table} -> ${apres_silver_table} lignes" \
  || fail "la table Silver n'a pas progressé"
[ "${apres_silver_topic:-0}" -gt "${avant_silver_topic:-0}" ] \
  && ok "topic silver-bank-transactions : ${avant_silver_topic} -> ${apres_silver_topic} offsets" \
  || fail "le topic Silver n'a pas progressé"

# --- 5. File de rebut ---------------------------------------------------------
step "5/10 — Les messages malformés partent en file de rebut"
avant_dlq=$(offsets dlq-financial-events)
printf 'ceci n est pas du json (%s)\n' "$(date +%s)" \
  | kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 --topic raw-bank-transactions
"${COMPOSE[@]}" exec -T spark python3 -m jobs.streaming.raw_to_silver \
  --once --datasets bank_txn >/dev/null 2>&1

apres_dlq=$(offsets dlq-financial-events)
[ "${apres_dlq:-0}" -gt "${avant_dlq:-0}" ] \
  && ok "dlq-financial-events : ${avant_dlq} -> ${apres_dlq} offsets" \
  || fail "le message malformé n'a pas atteint la file de rebut"

motifs=$(trino_sql "SELECT count(DISTINCT rejection_reason)
                    FROM kafka.default.\"dlq-financial-events\"
                    WHERE rejection_reason IS NOT NULL" | tr -dc '0-9')
[ "${motifs:-0}" -gt 0 ] && ok "${motifs} motif(s) de rejet distincts, exploitables en SQL" \
                         || fail "les rebuts ne portent aucun motif"

# --- 6. Détection de fraude, comparée à l'oracle -------------------------------
step "6/10 — Le Job 2 retrouve les anomalies injectées"
"${COMPOSE[@]}" exec -T spark python3 -m jobs.streaming.silver_to_gold --once 2>&1 \
  | grep -E "lot [0-9]+ —" | sed 's/^.*— /    /' || fail "le Job 2 a échoué"

for type_alerte in RAFALE_VIREMENTS ORIGINE_INHABITUELLE SINISTRE_EXCESSIF; do
  n=$(trino_sql "SELECT count(*) FROM iceberg.gold.fraud_alerts
                 WHERE alert_type = '${type_alerte}'" | tr -dc '0-9')
  [ "${n:-0}" -gt 0 ] && ok "${type_alerte} : ${n} alerte(s)" \
                      || fail "aucune alerte ${type_alerte} — la règle ne se déclenche pas"
done

# L'oracle de `generator.anomalies` rejoue les mêmes règles en pandas sur les
# fichiers encore présents dans la zone d'atterrissage. Les deux implémentations
# partagent leurs seuils, tirés de `common.domain`, mais aucune ligne de logique :
# l'une travaille sur un fichier, l'autre sur un flux fenêtré. C'est cet écart
# qui donne sa valeur à la concordance.
oracle=$("${COMPOSE[@]}" exec -T streamlit python -m generator.verify 2>/dev/null || true)
contains "Règle 1" "$oracle" || fail "l'oracle du générateur n'a pas produit de contrôle"
echo "$oracle" | grep -E "Règle [123]|AML" | sed 's/^/    /' >&2
contains "contrôles passent" "$oracle" \
  && ok "l'oracle retrouve les trois règles et l'AML dans les fichiers d'origine" \
  || fail "l'oracle signale des anomalies non détectables — la comparaison n'a plus de sens"

# --- 7. Surveillance AML ------------------------------------------------------
step "7/10 — Franchissements du seuil déclaratif"
aml=$(trino_sql "SELECT count(*) FROM iceberg.gold.aml_events" | tr -dc '0-9')
[ "${aml:-0}" -gt 0 ] && ok "${aml} événement(s) AML publiés" \
                      || fail "aucun événement AML"

devises=$(trino_sql "SELECT count(DISTINCT threshold) FROM iceberg.gold.aml_events" | tr -dc '0-9')
[ "${devises:-0}" -ge 1 ] && ok "seuil appliqué en devise locale (${devises} valeur(s) distincte(s))" \
                          || fail "le seuil AML n'est pas différencié par devise"

# --- 8. Requête Lambda unifiée ------------------------------------------------
step "8/10 — Requête Lambda unifiée"
bash scripts/queries_l1.sh level3 >/dev/null || fail "les requêtes du Level 3 échouent"
ok "les 5 requêtes du niveau s'exécutent"

# Le double comptage est le piège de cette requête : le batch et le flux
# décrivent les mêmes événements, et seule la borne de partage les sépare. Pour
# le vérifier, on consolide d'abord — la couche Gold rattrape alors tout le flux
# publié jusqu'ici — puis on exige que la vue temps réel soit devenue vide.
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.silver >/dev/null 2>&1
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.gold >/dev/null 2>&1

partage=$(trino_sql "
  WITH borne AS (
    SELECT coalesce(CAST(max(processed_at) AS timestamp(3)),
                    TIMESTAMP '1970-01-01 00:00:00.000') AS b
    FROM iceberg.gold.daily_transaction_volume)
  SELECT count_if(s.processed_at <= borne.b), count_if(s.processed_at > borne.b)
  FROM kafka.default.\"silver-bank-transactions\" s CROSS JOIN borne
  WHERE s.transaction_id IS NOT NULL")
read -r consolides frais <<<"$(echo "$partage" | tr '\t' ' ')"

[ "${consolides:-0}" -gt 0 ] \
  && ok "${consolides} événement(s) du topic déjà consolidés, écartés de la vue temps réel" \
  || fail "la borne n'écarte rien : le partage batch/streaming ne serait pas testé"
[ "${frais:-0}" = "0" ] \
  && ok "aucun double comptage après consolidation" \
  || fail "${frais} événement(s) compteraient deux fois"

# --- 9. Cohabitation batch et streaming ---------------------------------------
step "9/10 — Batch et streaming sur la même zone d'atterrissage"
restants=$("${COMPOSE[@]}" exec -T spark python3 -c "
from jobs.batch import landing
import os
client = landing.s3_client()
groupes = landing.list_pending(client, os.getenv('BUCKET_RAW', 'raw-landing'))
print(sum(len(v) for k, v in groupes.items() if k not in ('customers','accounts','branches','products')))
" 2>/dev/null | tr -dc '0-9')

"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.ingest_raw 2>&1 \
  | grep -E "laissé|jeu de données|^[a-z_]+ +[0-9]" | tail -6 | sed 's/^/    /' || true

apres=$("${COMPOSE[@]}" exec -T spark python3 -c "
from jobs.batch import landing
import os
client = landing.s3_client()
groupes = landing.list_pending(client, os.getenv('BUCKET_RAW', 'raw-landing'))
print(sum(len(v) for k, v in groupes.items() if k not in ('customers','accounts','branches','products')))
" 2>/dev/null | tr -dc '0-9')

# Les fichiers tout juste déposés bénéficient du délai de grâce : le batch les a
# ingérés sans les retirer, et NiFi peut encore les recenser.
[ "${apres:-0}" -gt 0 ] \
  && ok "délai de grâce respecté : ${apres} fichier(s) laissés au chemin streaming (${restants} avant)" \
  || fail "le batch a archivé des fichiers que NiFi n'a pas encore recensés"

# --- 10. Idempotence du rejeu -------------------------------------------------
step "10/10 — Rejouer les topics ne duplique rien"
silver_avant=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
fraude_avant=$(trino_sql "SELECT count(*) FROM iceberg.gold.fraud_alerts" | tr -dc '0-9')

"${COMPOSE[@]}" exec -T spark rm -rf /checkpoints/raw_to_silver /checkpoints/silver_to_gold
"${COMPOSE[@]}" exec -T spark python3 -m jobs.streaming.raw_to_silver --once >/dev/null 2>&1
"${COMPOSE[@]}" exec -T spark python3 -m jobs.streaming.silver_to_gold --once >/dev/null 2>&1

silver_apres=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
fraude_apres=$(trino_sql "SELECT count(*) FROM iceberg.gold.fraud_alerts" | tr -dc '0-9')

[ "$silver_avant" = "$silver_apres" ] && [ "$fraude_avant" = "$fraude_apres" ] \
  && ok "rejeu intégral sans duplication (${silver_apres} lignes Silver, ${fraude_apres} alertes)" \
  || fail "duplication au rejeu : Silver ${silver_avant}->${silver_apres}, alertes ${fraude_avant}->${fraude_apres}"

printf '\n\033[32m*** Level 3 opérationnel ***\033[0m\n\n'
