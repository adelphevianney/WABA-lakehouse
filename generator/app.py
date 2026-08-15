"""Application Streamlit de génération de données du WABA Group (Level 1.1).

L'interface ne contient que de la saisie et de l'affichage : tout ce qui décide
quoi générer et où le déposer vit dans `service.py`. Streamlit réexécute le
script entier à chaque interaction, ce qui rend indispensable de mettre en cache
les objets coûteux — le référentiel comptes fait 800 000 lignes et le recharger
depuis MinIO à chaque clic rendrait l'application inutilisable.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, time as dtime

import numpy as np
import pandas as pd
import streamlit as st

from common import pii
from generator import config as cfg
from generator import service, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

st.set_page_config(page_title="WABA — Générateur de données", page_icon="🏦", layout="wide")


# =============================================================================
# Ressources mises en cache
# =============================================================================


@st.cache_resource(show_spinner=False)
def get_store() -> storage.RawLandingStore:
    return storage.RawLandingStore()


@st.cache_resource(show_spinner="Chargement des référentiels depuis MinIO…")
def get_index(_version: int):
    """Index de génération. `_version` sert à invalider le cache après un rechargement."""
    return service.load_index(get_store())


def default_period() -> tuple[date, date]:
    """Dernier trimestre civil révolu, valeur par défaut imposée par l'énoncé."""
    today = date.today()
    start_current = date(today.year, 3 * ((today.month - 1) // 3) + 1, 1)
    end_previous = start_current - timedelta(days=1)
    start_previous = date(end_previous.year, 3 * ((end_previous.month - 1) // 3) + 1, 1)
    return start_previous, end_previous


# =============================================================================
# Barre latérale — état de la plateforme
# =============================================================================

st.sidebar.title("WABA Group")
st.sidebar.caption("Générateur de données — Level 1.1")

store = get_store()
reachable, message = store.is_reachable()

if reachable:
    st.sidebar.success(f"MinIO : {message}")
else:
    st.sidebar.error(f"MinIO injoignable — {message}")
    st.sidebar.info(
        "Démarrer la plateforme avec `make up-l1` (ou `.\\waba.ps1 up-l1`), "
        "puis recharger la page."
    )
    st.stop()

if pii.is_using_fallback_key():
    st.sidebar.warning(
        "Pseudonymisation active avec la clé de repli de développement. "
        "Définir `WABA_PII_KEY` pour un usage réel."
    )

st.session_state.setdefault("index_version", 0)
referentials_ready = store.referentials_present()

if referentials_ready:
    st.sidebar.info("Référentiels présents dans le bucket")
    inventory = store.inventory()
    if not inventory.empty:
        st.sidebar.markdown("**Fichiers déposés**")
        st.sidebar.dataframe(inventory, hide_index=True, use_container_width=True)
else:
    st.sidebar.warning("Aucun référentiel : commencer par les générer")


# =============================================================================
# Section 1 — Référentiels
# =============================================================================

st.title("Génération de données financières simulées")
st.caption(
    "8 pays d'Afrique de l'Ouest · 4 lignes métier · dépôt direct dans le bucket "
    f"`{store.settings.raw_bucket}` de MinIO"
)

with st.expander("Référentiels (clients, comptes, agences, produits)", expanded=not referentials_ready):
    st.markdown(
        "Les référentiels sont générés **une seule fois** et partagés entre pays. "
        "Toutes les transactions ne référencent que leurs clés : les régénérer "
        "rendrait orphelines les clés des fichiers déjà déposés."
    )

    columns = st.columns(4)
    sizes = {
        name: columns[position].number_input(
            name, min_value=10, max_value=2_000_000,
            value=cfg.REFERENTIAL_SIZES[name], step=1000, key=f"size_{name}",
        )
        for position, name in enumerate(("customers", "accounts", "branches", "products"))
    }

    confirm = True
    if referentials_ready:
        confirm = st.checkbox(
            "Je confirme vouloir écraser les référentiels existants "
            "(les fichiers de transactions déjà déposés deviendront incohérents)"
        )

    if st.button("Générer les référentiels", type="primary", disabled=not confirm):
        with st.status("Génération en cours…", expanded=True) as status:
            started = time.perf_counter()
            frames = service.build_referentials(store, sizes)
            st.session_state["index_version"] += 1
            get_index.clear()
            elapsed = time.perf_counter() - started
            for name, frame in frames.items():
                st.write(f"`{name}` — {len(frame):,} lignes déposées")
            status.update(label=f"Référentiels générés en {elapsed:.1f} s", state="complete")
        st.rerun()

if not referentials_ready:
    st.stop()


# =============================================================================
# Section 2 — Transactions
# =============================================================================

st.subheader("Flux transactionnels")

left, right = st.columns([2, 1])

with left:
    selected_kinds = st.multiselect(
        "Type de données",
        options=list(service.KIND_LABELS),
        default=["bank_txn"],
        format_func=lambda k: service.KIND_LABELS[k],
    )
    selected_entities = st.multiselect(
        "Ligne métier",
        options=list(cfg.ENTITY_TYPES),
        default=list(cfg.ENTITY_TYPES),
        help="Restreint les comptes mouvementés aux entités retenues.",
    )

    # On ne propose que les pays où les types choisis peuvent réellement exister.
    available = sorted({
        country
        for kind in selected_kinds
        for country in service.eligible_countries(kind, tuple(selected_entities))
    })
    selected_countries = st.multiselect(
        "Pays",
        options=available or list(cfg.COUNTRY_CODES),
        default=available,
        format_func=lambda c: f"{c} — {cfg.COUNTRIES[c].name}",
        help="Seuls les pays où les entités sélectionnées opèrent sont proposés.",
    )

with right:
    start_default, end_default = default_period()
    period = st.date_input(
        "Période simulée",
        value=(start_default, end_default),
        help="Par défaut, le dernier trimestre civil révolu.",
    )
    # Le curseur travaille en pourcentage : Streamlit applique le format à la
    # valeur brute, et une fraction 0,02 s'afficherait « 0,0 % ».
    anomaly_percent = st.slider(
        "Taux d'anomalies injectées", 0.0, 10.0, cfg.DEFAULT_ANOMALY_RATE * 100, 0.5,
        format="%.1f %%",
        help=(
            "Rafales de virements, paiements depuis un pays inhabituel, sinistres "
            "disproportionnés. Sans injection, aucune règle de fraude du Level 3 "
            "ne se déclenche."
        ),
    )
    anomaly_rate = anomaly_percent / 100

if selected_kinds:
    st.markdown("**Nombre de lignes par fichier**")
    row_columns = st.columns(len(selected_kinds))
    row_counts = {
        kind: row_columns[position].number_input(
            service.KIND_LABELS[kind], min_value=1, max_value=1_000_000,
            value=cfg.DEFAULT_ROWS[kind], step=500, key=f"rows_{kind}",
        )
        for position, kind in enumerate(selected_kinds)
    }
else:
    row_counts = {}

mode = st.radio(
    "Mode de génération",
    options=("one-time", "continue"),
    horizontal=True,
    format_func=lambda m: (
        "Ponctuel — toute la période, un fichier par jour"
        if m == "one-time"
        else "Continu — flux temps réel sur le dernier jour de la période"
    ),
)
interval = (
    st.slider("Intervalle entre deux lots (secondes)", 10, 60, 15)
    if mode == "continue"
    else 0
)


def build_request(single_day: bool = False) -> service.GenerationRequest:
    """Construit la demande de génération.

    `single_day` sert au mode continu : chaque cycle ne produit qu'une journée,
    celle de fin de période. Rejouer l'ensemble de la période à chaque cycle
    déposerait 90 fichiers par pays toutes les quinze secondes.
    """
    start, end = (period if isinstance(period, tuple) and len(period) == 2
                  else (start_default, end_default))
    if single_day:
        start = end
    return service.GenerationRequest(
        kinds=tuple(selected_kinds),
        countries=tuple(selected_countries),
        entity_types=tuple(selected_entities),
        rows=row_counts,
        start=datetime.combine(start, dtime.min),
        end=datetime.combine(end, dtime.max),
        anomaly_rate=anomaly_rate,
    )


def render_results(results: list[service.BatchResult]) -> None:
    produced = [r for r in results if r.ok]
    skipped = [r for r in results if not r.ok]

    if produced:
        st.dataframe(
            pd.DataFrame([
                {
                    "Pays": r.country_code,
                    "Type": service.KIND_LABELS[r.kind],
                    "Fichiers": r.files,
                    "Lignes": r.rows,
                    "Dernier fichier": r.key,
                    "Anomalies": sum(a.rows for a in r.anomalies),
                }
                for r in produced
            ]),
            hide_index=True, use_container_width=True,
        )
        details = [a for r in produced for a in r.anomalies if a.rows]
        if details:
            with st.expander(f"Anomalies injectées ({sum(a.rows for a in details):,} lignes)"):
                for report in details:
                    st.write(f"**{report.rule}** — {report.detail}")

    for result in skipped:
        st.warning(f"{result.country_code} / {service.KIND_LABELS[result.kind]} ignoré : {result.skipped_reason}")


ready = bool(selected_kinds and selected_countries)
if not ready:
    st.info("Sélectionner au moins un type de données et un pays.")
else:
    # Un fichier par pays et par journée : sur un trimestre, la demande porte
    # vite sur plusieurs centaines de fichiers. L'annoncer avant de lancer évite
    # la surprise, d'autant que l'ingestion Spark devra tous les relire.
    planned = build_request(single_day=(mode == "continue"))
    total_files = planned.file_count()
    total_rows = sum(
        row_counts.get(kind, cfg.DEFAULT_ROWS[kind]) * len(planned.days())
        * len(set(selected_countries) & set(service.eligible_countries(kind, tuple(selected_entities))))
        for kind in selected_kinds
    )

    if mode == "continue":
        # Sans cette précision, un utilisateur ayant sélectionné un trimestre
        # s'attend à voir des fichiers couvrir toute la période, alors que le
        # flux alimente uniquement sa journée la plus récente.
        st.info(
            f"Un flux temps réel alimente le jour courant : **chaque cycle dépose "
            f"{total_files} fichiers datés du {planned.days()[0]:%d/%m/%Y}**, dernier jour "
            f"de la période, avec un numéro de séquence incrémenté. Les autres journées "
            f"de la période sélectionnée ne sont pas alimentées dans ce mode."
        )
    else:
        message = (
            f"{len(planned.days())} journée(s) simulée(s) → **{total_files} fichiers**, "
            f"environ {total_rows:,} lignes.".replace(",", " ")
        )
        if total_files > 200:
            st.warning(message + " Volume important : la génération et l'ingestion prendront du temps.")
        else:
            st.info(message)

# --- Mode ponctuel -----------------------------------------------------------

if mode == "one-time":
    st.session_state["continuous"] = False
    if st.button("Générer et déposer dans MinIO", type="primary", disabled=not ready):
        index = get_index(st.session_state["index_version"])
        with st.spinner("Génération et dépôt…"):
            results = service.run_batch(
                index, store, build_request(), np.random.default_rng()
            )
        st.success(f"{sum(r.rows for r in results if r.ok):,} lignes déposées")
        render_results(results)

# --- Mode continu ------------------------------------------------------------

else:
    st.session_state.setdefault("continuous", False)
    st.session_state.setdefault("cycles", 0)
    st.session_state.setdefault("history", [])

    start_column, stop_column = st.columns(2)
    if start_column.button("Démarrer le flux", type="primary", disabled=not ready):
        st.session_state["continuous"] = True
        st.session_state["cycles"] = 0
        st.session_state["history"] = []
    if stop_column.button("Arrêter"):
        st.session_state["continuous"] = False

    if st.session_state["continuous"]:
        index = get_index(st.session_state["index_version"])
        results = service.run_batch(
            index, store, build_request(single_day=True), np.random.default_rng()
        )
        st.session_state["cycles"] += 1
        st.session_state["history"].insert(0, {
            "Cycle": st.session_state["cycles"],
            "Heure": datetime.now().strftime("%H:%M:%S"),
            "Fichiers": sum(1 for r in results if r.ok),
            "Lignes": sum(r.rows for r in results if r.ok),
        })

        st.success(
            f"Flux actif — cycle {st.session_state['cycles']}, "
            f"journée alimentée : {build_request(single_day=True).days()[0]:%d/%m/%Y}"
        )
        render_results(results)
        st.dataframe(
            pd.DataFrame(st.session_state["history"][:20]),
            hide_index=True, use_container_width=True,
        )
        # L'arrêt est pris en compte à la fin du cycle en cours : Streamlit met
        # les interactions en file d'attente pendant l'exécution du script.
        st.caption(f"Prochain lot dans {interval} s — l'arrêt prend effet en fin de cycle.")
        time.sleep(interval)
        st.rerun()
    elif st.session_state["cycles"]:
        st.info(f"Flux arrêté après {st.session_state['cycles']} cycle(s).")
        st.dataframe(
            pd.DataFrame(st.session_state["history"][:20]),
            hide_index=True, use_container_width=True,
        )
