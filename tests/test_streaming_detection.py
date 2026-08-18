"""Contrats du job de détection Silver vers Gold.

Les règles elles-mêmes sont validées contre l'oracle de `generator.anomalies`,
qui en donne une implémentation de référence en pandas : c'est le rôle du test
de fumée, qui compare les alertes produites aux anomalies injectées. Restent les
constantes et les correspondances qui, si elles dérivaient, ne produiraient
aucune erreur — un topic jamais alimenté, une fenêtre du mauvais format, un
seuil appliqué dans la mauvaise devise.
"""

from __future__ import annotations

import pytest

from common import domain as dom
from jobs.batch import silver
from jobs.streaming import silver_to_gold as job


@pytest.mark.parametrize("dataset", sorted(dom.SILVER_TOPICS))
def test_chaque_topic_silver_est_surveille(dataset):
    """Un topic alimenté par le Job 1 mais surveillé par personne laisserait
    passer les fraudes qu'il transporte, sans la moindre erreur."""
    assert dataset in job.SILVER_TABLE_OF
    assert dataset in job.KEY_OF


@pytest.mark.parametrize("dataset, table", sorted(job.SILVER_TABLE_OF.items()))
def test_le_schema_de_lecture_vient_de_la_table_silver(dataset, table):
    """Le schéma du topic n'est pas redéclaré : il est lu sur la table que le
    Job 1 alimente avec le même DataFrame."""
    assert table in silver.BY_NAME


@pytest.mark.parametrize("dataset, key", sorted(job.KEY_OF.items()))
def test_la_cle_naturelle_est_celle_de_la_table(dataset, key):
    assert key == silver.BY_NAME[job.SILVER_TABLE_OF[dataset]].key


def test_la_fenetre_glissante_est_celle_de_l_enonce():
    """5 minutes glissant d'une minute : ce n'est pas un réglage libre, l'énoncé
    le fixe."""
    assert job.WINDOW_SIZE == "5 minutes"
    assert job.WINDOW_SLIDE == "1 minute"


def test_le_seuil_de_rafale_est_exprime_par_devise():
    """500 000 XOF et 2 500 GHS ne sont pas le même montant : convertir avant de
    comparer appliquerait au Ghana un seuil calibré pour la zone franc."""
    assert set(dom.FRAUD_BURST_AMOUNT) == set(dom.CURRENCIES)
    assert dom.FRAUD_BURST_AMOUNT["XOF"] == 500_000.0
    assert dom.FRAUD_BURST_AMOUNT["GHS"] == 2_500.0


def test_le_seuil_aml_est_celui_de_la_bceao():
    assert dom.AML_THRESHOLD["XOF"] == 1_000_000.0
    assert dom.AML_THRESHOLD["GHS"] == 5_000.0


def test_la_rafale_compte_au_moins_trois_operations():
    assert dom.FRAUD_BURST_MIN_COUNT == 3
    assert dom.FRAUD_BURST_WINDOW_MINUTES == 5


def test_le_multiple_de_prime_est_celui_de_la_regle():
    assert dom.FRAUD_CLAIM_PREMIUM_RATIO == 3.0


def test_les_trois_topics_d_alerte_sont_ceux_du_domaine():
    """Publier dans un topic inexistant ne lève pas d'erreur côté Kafka : le
    broker le crée à la volée, et personne ne consomme le bon."""
    assert dom.GOLD_FRAUD_TOPIC == "gold-fraud-alerts"
    assert dom.GOLD_AML_TOPIC == "gold-aml-events"
    assert dom.GOLD_LIQUIDITY_TOPIC == "gold-liquidity-alerts"


def test_les_trois_regles_sont_activables_separement():
    assert set(job.RULES) == {"unitaires", "rafales", "liquidite"}


def test_le_seuil_de_liquidite_par_defaut_est_celui_du_domaine():
    """La valeur réglementaire vit dans le domaine ; le job ne fait que la lire,
    et sa surcharge par variable d'environnement reste explicite."""
    assert job.LIQUIDITY_RATIO == pytest.approx(dom.LIQUIDITY_OUTFLOW_RATIO)


def test_les_tables_gold_du_streaming_ne_recouvrent_pas_celles_du_batch():
    """Le Level 2 produit sept tables Gold de KPIs ; le Level 3 en ajoute trois
    d'alertes. Une collision de nom ferait écrire deux jobs dans la même table
    avec des schémas différents."""
    from jobs.batch import gold

    batch = {table.name for table in gold.TABLES}
    streaming = {job.FRAUD_TABLE, job.AML_TABLE, job.LIQUIDITY_TABLE}
    assert batch.isdisjoint(streaming)
