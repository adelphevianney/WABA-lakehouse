"""Flux transactionnels : schémas, intégrité référentielle, cohérence métier."""

from __future__ import annotations

import pytest

from generator import config as cfg
from generator import transactions as txn
from tests.conftest import PERIOD_END, PERIOD_START

ANNEXE = {
    "bank_txn": {"transaction_id", "timestamp", "account_id", "beneficiary_account",
                 "branch_id", "country_code", "transaction_type", "amount", "currency",
                 "channel", "transaction_status", "fee_amount", "entity_type"},
    "insurance_ops": {"operation_id", "timestamp", "customer_id", "account_id",
                      "country_code", "operation_type", "product_line", "amount",
                      "currency", "claim_status", "processing_days", "entity_type"},
    "mobile_money": {"payment_id", "timestamp", "sender_id", "receiver_id",
                     "sender_country", "receiver_country", "amount", "currency",
                     "payment_type", "operator", "status", "fee_amount", "entity_type"},
    "loan_repayments": {"repayment_id", "timestamp", "loan_account_id", "customer_id",
                        "country_code", "amount_due", "amount_paid", "currency",
                        "due_date", "payment_date", "days_overdue", "loan_type",
                        "repayment_status", "entity_type"},
}


def generate(index, kind, country="CI", rows=800, rng=None):
    import numpy as np
    return txn.GENERATORS[kind](
        index, country, rows, PERIOD_START, PERIOD_END, rng or np.random.default_rng(1)
    )


@pytest.mark.parametrize("kind", sorted(ANNEXE))
def test_schema_conforme_a_l_annexe(index, kind):
    country = "CI" if kind != "loan_repayments" else "ML"
    assert ANNEXE[kind] <= set(generate(index, kind, country).columns)


@pytest.mark.parametrize("kind", sorted(ANNEXE))
def test_colonne_country_code_presente(index, kind):
    """Contrainte transverse : toutes les tables doivent la porter."""
    assert "country_code" in generate(index, kind).columns


@pytest.mark.parametrize("kind", sorted(ANNEXE))
def test_volumetrie_demandee_respectee(index, kind):
    assert len(generate(index, kind, rows=613)) == 613


def test_aucune_cle_orpheline(index, referentials):
    comptes = set(referentials["accounts"]["account_id"])
    clients = set(referentials["customers"]["customer_id"])
    agences = set(referentials["branches"]["branch_id"])

    banque = generate(index, "bank_txn")
    assert banque["account_id"].isin(comptes).all()
    assert banque["beneficiary_account"].isin(comptes).all()
    assert banque["branch_id"].isin(agences).all()

    assurance = generate(index, "insurance_ops")
    assert assurance["customer_id"].isin(clients).all()
    assert assurance["account_id"].isin(comptes).all()

    mobile = generate(index, "mobile_money")
    assert mobile["sender_id"].isin(clients).all()
    assert mobile["receiver_id"].isin(clients).all()

    prets = generate(index, "loan_repayments")
    assert prets["loan_account_id"].isin(comptes).all()
    assert prets["customer_id"].isin(clients).all()


def test_agence_de_la_meme_entite_que_le_compte_debite(index, referentials):
    """Joindre transactions et agences ne doit pas révéler d'incohérence."""
    banque = generate(index, "bank_txn", rows=2000)
    entite_agence = referentials["branches"].set_index("branch_id")["entity_type"]
    assert (banque["branch_id"].map(entite_agence) == banque["entity_type"]).all()


def test_lien_compte_client_preserve_sur_les_remboursements(index, referentials):
    prets = generate(index, "loan_repayments", country="ML")
    proprietaire = referentials["accounts"].set_index("account_id")["customer_id"]
    assert (prets["loan_account_id"].map(proprietaire) == prets["customer_id"]).all()


@pytest.mark.parametrize("country", ["CI", "GH"])
def test_devise_conforme_au_pays(index, country):
    banque = generate(index, "bank_txn", country)
    assert set(banque["currency"]) == {cfg.CURRENCY_BY_COUNTRY[country]}


def test_horodatages_dans_la_periode_demandee(index):
    banque = generate(index, "bank_txn")
    assert banque["timestamp"].min() >= PERIOD_START
    assert banque["timestamp"].max() <= PERIOD_END


def test_frais_nuls_si_l_operation_echoue(index):
    banque = generate(index, "bank_txn", rows=3000)
    echecs = banque[banque["transaction_status"] != "SUCCESS"]
    assert (echecs["fee_amount"] == 0).all()


def test_virements_internationaux_sortent_du_pays(index, referentials):
    banque = generate(index, "bank_txn", rows=4000)
    wires = banque[banque["transaction_type"] == "INTERNATIONAL_WIRE"]
    pays_beneficiaire = referentials["accounts"].set_index("account_id")["country_code"]
    assert (wires["beneficiary_account"].map(pays_beneficiaire) != "CI").all()


def test_delai_de_traitement_reserve_aux_sinistres(index):
    assurance = generate(index, "insurance_ops", rows=3000)
    hors_sinistre = assurance[~assurance["operation_type"].isin(("CLAIM_SUBMISSION", "CLAIM_PAYMENT"))]
    assert hors_sinistre["processing_days"].isna().all()
    assert hors_sinistre["claim_status"].isna().all()


def test_branches_vie_absentes_du_ghana(index):
    assurance = generate(index, "insurance_ops", country="GH", rows=2000)
    assert not assurance["product_line"].isin(("VIE", "PREVOYANCE")).any()


def test_microcredit_reserve_aux_pays_de_la_microfinance(index):
    prets = generate(index, "loan_repayments", country="CI", rows=2000)
    assert not prets["loan_type"].isin(("MICROCREDIT", "AGRICULTURAL")).any()


def test_mobile_money_impossible_hors_perimetre(index):
    """Le Mali n'a pas d'entité mobile money : la génération doit refuser."""
    with pytest.raises(ValueError, match="référentiel vide"):
        generate(index, "mobile_money", country="ML")


def test_corridors_transfrontaliers_produits(index):
    mobile = generate(index, "mobile_money", rows=5000)
    transfrontalier = mobile[mobile["sender_country"] != mobile["receiver_country"]]
    assert len(transfrontalier) > 0
    assert set(transfrontalier["payment_type"]) == {"CROSS_BORDER_TRANSFER"}


def test_statut_de_remboursement_coherent_avec_le_retard(index):
    prets = generate(index, "loan_repayments", rows=3000)
    en_defaut = prets["repayment_status"] == "DEFAULT"
    assert (prets.loc[en_defaut, "days_overdue"] > 90).all()
    assert (prets.loc[en_defaut, "amount_paid"] == 0).all()
    assert prets.loc[en_defaut, "payment_date"].isna().all()
    assert (prets.loc[~en_defaut, "days_overdue"] <= 90).all()


def test_identifiants_de_transaction_uniques(index):
    banque = generate(index, "bank_txn", rows=5000)
    assert not banque["transaction_id"].duplicated().any()


def test_filtre_par_ligne_metier(index):
    import numpy as np
    prets = txn.generate_loan_repayments(
        index, "ML", 500, PERIOD_START, PERIOD_END, np.random.default_rng(1),
        entity_types=("MICROFINANCE",),
    )
    assert set(prets["entity_type"]) == {"MICROFINANCE"}
