"""Injection et détection des motifs de fraude.

Chaque règle est vérifiée dans les deux sens : elle ne trouve rien sur des
données non altérées, et elle trouve quelque chose après injection. C'est la
propriété qui garantit que les alertes du Level 3 auront de la matière, et que
la règle discrimine au lieu de tout signaler.
"""

from __future__ import annotations

import numpy as np
import pytest

from generator import anomalies as ano
from generator import config as cfg
from generator import transactions as txn
from tests.conftest import PERIOD_END, PERIOD_START

TAUX = 0.03


def generate(index, kind, country="CI", rows=3000, seed=2):
    return txn.GENERATORS[kind](
        index, country, rows, PERIOD_START, PERIOD_END, np.random.default_rng(seed)
    )


# --- Règle 1 : rafales -------------------------------------------------------


def test_aucune_rafale_sans_injection(index):
    """Le point central : une rafale ne survient jamais par hasard, puisque
    chaque ligne tire son compte indépendamment."""
    assert len(ano.detect_transaction_bursts(generate(index, "bank_txn"))) == 0


def test_rafales_detectees_apres_injection(index, rng):
    brut = generate(index, "bank_txn")
    altere, rapport = ano.inject_transaction_bursts(brut, "CI", TAUX, rng)
    assert rapport.rows > 0
    assert len(ano.detect_transaction_bursts(altere)) >= rapport.rows


def test_rafale_respecte_montant_et_fenetre(index, rng):
    altere, _ = ano.inject_transaction_bursts(generate(index, "bank_txn"), "CI", TAUX, rng)
    detectees = ano.detect_transaction_bursts(altere)
    assert (detectees["amount"] > cfg.FRAUD_BURST_AMOUNT["XOF"]).all()
    for _, groupe in detectees.groupby("account_id"):
        etendue = groupe["timestamp"].max() - groupe["timestamp"].min()
        assert etendue <= np.timedelta64(cfg.FRAUD_BURST_WINDOW_MINUTES, "m")


# --- Règle 2 : origine géographique inhabituelle -----------------------------


def test_aucun_paiement_etranger_sans_injection(index, referentials):
    mobile = generate(index, "mobile_money")
    assert len(ano.detect_foreign_origin_payments(mobile, referentials["customers"])) == 0


def test_paiements_etrangers_detectes_apres_injection(index, referentials, rng):
    brut = generate(index, "mobile_money")
    altere, rapport = ano.inject_foreign_origin_payments(brut, index, "CI", TAUX, rng)
    detectes = ano.detect_foreign_origin_payments(altere, referentials["customers"])
    assert rapport.rows > 0
    assert len(detectes) == rapport.rows


# --- Règle 3 : sinistre disproportionné --------------------------------------


def test_aucun_sinistre_excessif_sans_injection(index):
    """Un sinistre vaut normalement une fraction de la prime annuelle : la règle
    des 3x ne doit pas se déclencher sur le flux normal, sinon elle ne
    discrimine rien."""
    assert len(ano.detect_excessive_claims(generate(index, "insurance_ops"))) == 0


def test_sinistres_excessifs_detectes_apres_injection(index, rng):
    brut = generate(index, "insurance_ops")
    altere, rapport = ano.inject_excessive_claims(brut, "CI", TAUX, rng)
    assert rapport.rows > 0
    assert len(ano.detect_excessive_claims(altere)) == rapport.rows


# --- AML ---------------------------------------------------------------------


def test_aml_ne_retient_que_les_virements(index):
    banque = generate(index, "bank_txn", rows=5000)
    detectes = ano.detect_aml_events(banque)
    assert set(detectes["transaction_type"]) <= {"TRANSFER", "INTERNATIONAL_WIRE"}
    seuil = detectes["currency"].map(cfg.AML_THRESHOLD)
    assert (detectes["amount"] > seuil).all()


def test_aml_n_injecte_rien_quand_le_volume_naturel_suffit(index, rng):
    banque = generate(index, "bank_txn", rows=5000)
    avant = len(ano.detect_aml_events(banque))
    altere, rapport = ano.ensure_aml_events(banque, "CI", minimum=5, rng=rng)
    assert rapport.rows == 0, "compter les événements naturels gonflerait le taux d'anomalies"
    assert len(ano.detect_aml_events(altere)) == avant


def test_aml_complete_les_lots_trop_petits(index, rng):
    banque = generate(index, "bank_txn", rows=40)
    altere, rapport = ano.ensure_aml_events(banque, "CI", minimum=12, rng=rng)
    assert len(ano.detect_aml_events(altere)) >= 12
    assert rapport.rows > 0


# --- Propriétés transverses --------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "country"), [("bank_txn", "CI"), ("mobile_money", "CI"), ("insurance_ops", "GH")]
)
def test_injection_preserve_schema_et_volumetrie(index, rng, kind, country):
    brut = generate(index, kind, country)
    altere, _ = ano.inject(kind, brut, index, country, TAUX, rng)
    assert list(altere.columns) == list(brut.columns)
    assert len(altere) == len(brut)


def test_injection_n_ajoute_aucune_colonne_indicatrice(index, rng):
    """Marquer les lignes frauduleuses donnerait la réponse au pipeline de
    détection et ferait diverger le schéma de l'annexe."""
    brut = generate(index, "bank_txn")
    altere, _ = ano.inject("bank_txn", brut, index, "CI", TAUX, rng)
    assert not {"is_fraud", "is_anomaly", "fraud_label"} & set(altere.columns)


def test_injection_ne_cree_pas_de_cle_orpheline(index, referentials, rng):
    brut = generate(index, "bank_txn")
    altere, _ = ano.inject("bank_txn", brut, index, "CI", TAUX, rng)
    assert altere["account_id"].isin(set(referentials["accounts"]["account_id"])).all()


def test_taux_nul_laisse_les_donnees_intactes(index, rng):
    brut = generate(index, "bank_txn")
    altere, rapports = ano.inject("bank_txn", brut, index, "CI", 0.0, rng)
    assert rapports == []
    assert altere.equals(brut)
