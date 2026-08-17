"""Contrats du job de streaming Raw vers Silver.

Un job de streaming ne se teste pas entièrement hors infrastructure : il faut un
broker, un catalogue et une session Spark, et c'est le rôle du test de fumée.
Restent les contrats qui, s'ils se rompaient, ne produiraient aucune erreur —
seulement un topic vide, une table qui n'avance plus, ou un message rejeté sans
motif exploitable. Ce sont ceux-là qui sont vérifiés ici.
"""

from __future__ import annotations

import pytest

from common.domain import DATASETS, RAW_TOPICS, SILVER_TOPICS
from jobs.batch import schemas as sch
from jobs.batch import silver
from jobs.streaming import raw_to_silver as job


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_chaque_flux_lit_un_topic_declare(stream):
    assert stream.raw_topic == RAW_TOPICS[stream.dataset]


def test_les_quatre_topics_bruts_sont_consommes():
    """Un topic alimenté par NiFi mais consommé par personne s'accumulerait sans
    la moindre erreur, jusqu'à saturer le broker."""
    assert {s.raw_topic for s in job.STREAMS} == set(RAW_TOPICS.values())


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_la_table_silver_visee_existe_dans_le_chemin_batch(stream):
    """Les deux chemins alimentent les mêmes tables : viser un nom absent du
    catalogue batch créerait une table parallèle, invisible dans les KPIs."""
    assert stream.silver_table in silver.BY_NAME
    assert stream.builder is silver.BY_NAME[stream.silver_table].builder


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_le_topic_silver_suit_la_nomenclature_de_l_enonce(stream):
    assert stream.silver_topic == SILVER_TOPICS.get(stream.dataset)


def test_les_remboursements_n_ont_pas_de_topic_silver():
    """L'énoncé n'en prévoit que trois. L'absence est intentionnelle : aucune
    règle du Job 2 ne porte sur les remboursements de crédit."""
    assert job.BY_DATASET["loan_repayments"].silver_topic is None
    assert len([s for s in job.STREAMS if s.silver_topic]) == 3


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_le_schema_de_lecture_couvre_les_colonnes_et_la_tracabilite(stream):
    """Une colonne absente du schéma de lecture serait silencieusement perdue :
    `from_json` ignore ce qu'il ne connaît pas."""
    champs = [f.name for f in job.json_schema(stream.spec).fields]
    attendus = [c.name for c in stream.spec.all_columns]
    assert champs[:len(attendus)] == attendus
    assert sch.INGESTION_TIMESTAMP in champs and sch.SOURCE_FILE in champs


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_le_schema_de_lecture_est_entierement_textuel(stream):
    """Le typage vient après, pour distinguer une valeur absente d'une valeur
    présente mais non convertible — deux motifs de rejet différents."""
    assert {f.dataType.simpleString() for f in job.json_schema(stream.spec).fields} == {"string"}


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_le_schema_declare_la_colonne_de_rebut(stream):
    """Sans elle, un message illisible ressort en champs tous nuls et reçoit un
    motif de rejet trompeur au lieu du bon."""
    champs = [f.name for f in job.json_schema(stream.spec).fields]
    assert champs[-1] == job.CORRUPT_JSON
    assert job.CORRUPT_JSON not in [c.name for c in stream.spec.all_columns]


def test_la_fenetre_de_deduplication_est_celle_de_l_enonce():
    assert job.DEDUP_WINDOW == "10 minutes"


def test_chaque_jeu_de_donnees_transactionnel_est_couvert():
    transactionnels = set(DATASETS) & set(RAW_TOPICS)
    assert set(job.BY_DATASET) == transactionnels


@pytest.mark.parametrize("stream", job.STREAMS, ids=lambda s: s.dataset)
def test_la_cle_de_deduplication_est_la_cle_naturelle(stream):
    """C'est elle qui porte l'idempotence, du `MERGE` Iceberg à la fenêtre
    glissante : les deux doivent parler de la même colonne."""
    assert stream.spec.key == silver.BY_NAME[stream.silver_table].key
