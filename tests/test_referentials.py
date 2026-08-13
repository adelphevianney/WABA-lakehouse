"""Intégrité et cohérence métier des référentiels."""

from __future__ import annotations

import pytest

from generator import config as cfg
from generator import referentials as ref

ANNEXE_A1 = {"customer_id", "country_code", "entity_type", "segment",
             "kyc_level", "onboarding_date", "region", "is_active"}
ANNEXE_A2 = {"account_id", "customer_id", "country_code", "account_type",
             "currency", "balance", "credit_limit", "opened_date", "status"}
ANNEXE_A3 = {"branch_id", "country_code", "entity_type", "city", "region",
             "branch_type", "is_active"}


def test_schema_customers_conforme_a_l_annexe(referentials):
    assert ANNEXE_A1 <= set(referentials["customers"].columns)


def test_schema_accounts_conforme_a_l_annexe(referentials):
    """L'annexe est un minimum : iban et account_number sont ajoutés car les
    contraintes transverses et le Level 4 les exigent sans les déclarer."""
    columns = set(referentials["accounts"].columns)
    assert ANNEXE_A2 <= columns
    assert {"iban", "account_number"} <= columns


def test_schema_branches_conforme_a_l_annexe(referentials):
    assert ANNEXE_A3 <= set(referentials["branches"].columns)


def test_aucun_compte_orphelin(referentials):
    """Contrainte explicite de l'énoncé : aucune clé orpheline tolérée."""
    connus = set(referentials["customers"]["customer_id"])
    assert (~referentials["accounts"]["customer_id"].isin(connus)).sum() == 0


def test_chaque_client_detient_au_moins_un_compte(referentials):
    sans_compte = set(referentials["customers"]["customer_id"]) - set(referentials["accounts"]["customer_id"])
    assert sans_compte == set()


@pytest.mark.parametrize("table", ["customers", "accounts", "branches", "products"])
def test_identifiants_uniques(referentials, table):
    identifiant = {"customers": "customer_id", "accounts": "account_id",
                   "branches": "branch_id", "products": "product_id"}[table]
    assert not referentials[table][identifiant].duplicated().any()


@pytest.mark.parametrize("table", ["customers", "accounts", "branches"])
def test_matrice_pays_entite_respectee(referentials, table):
    frame = referentials[table]
    for entity, groupe in frame.groupby("entity_type"):
        autorises = set(cfg.countries_of(str(entity)))
        assert set(groupe["country_code"]) <= autorises, f"{table}/{entity}"


def test_devise_deduite_du_pays(referentials):
    comptes = referentials["accounts"]
    attendue = comptes["country_code"].map(cfg.CURRENCY_BY_COUNTRY)
    assert (comptes["currency"] == attendue).all()


def test_montants_a_l_echelle_de_leur_devise(referentials):
    """Un solde ghanéen et un solde ivoirien ne peuvent pas avoir le même ordre
    de grandeur : le rapport doit refléter la parité des deux devises."""
    medianes = referentials["accounts"].groupby("currency")["balance"].median()
    rapport = medianes["XOF"] / medianes["GHS"]
    attendu = cfg.XOF_PER_EUR / cfg.GHS_PER_EUR
    assert rapport == pytest.approx(attendu, rel=0.5)


def test_aucun_compte_ouvert_avant_l_entree_en_relation(referentials):
    joint = referentials["accounts"].merge(
        referentials["customers"][["customer_id", "onboarding_date"]], on="customer_id"
    )
    assert (joint["opened_date"] >= joint["onboarding_date"]).all()


def test_iban_absent_au_ghana_et_present_ailleurs(referentials):
    """Le Ghana ne fait pas partie du registre IBAN : ce null est métier."""
    comptes = referentials["accounts"]
    ghaneens = comptes[comptes["country_code"] == "GH"]
    autres = comptes[comptes["country_code"] != "GH"]
    assert ghaneens["iban"].isna().all()
    assert autres["iban"].notna().all()
    assert (autres["iban"].str.len() == 28).all()


def test_plafond_de_credit_nul_hors_credit_et_compte_courant(referentials):
    comptes = referentials["accounts"]
    sans_credit = comptes[~comptes["account_type"].isin(("LOAN", "CURRENT"))]
    assert (sans_credit["credit_limit"] == 0).all()


def test_type_de_compte_coherent_avec_l_entite(referentials):
    comptes = referentials["accounts"]
    assert set(comptes.loc[comptes["entity_type"] == "MOBILE_MONEY", "account_type"]) == {"MOBILE_WALLET"}
    assert set(comptes.loc[comptes["entity_type"] == "INSURANCE", "account_type"]) == {"INSURANCE_POLICY"}


def test_chaque_couple_pays_entite_possede_une_agence(referentials):
    """Sans cette garantie, une transaction pourrait n'avoir aucune agence
    valide à référencer — la clé orpheline que l'énoncé interdit."""
    couples = set(map(tuple, referentials["branches"][["country_code", "entity_type"]].to_numpy()))
    attendus = {(pays, entite) for entite in cfg.ENTITY_TYPES for pays in cfg.countries_of(entite)}
    assert attendus <= couples


def test_refuse_de_generer_moins_d_agences_que_de_couples():
    with pytest.raises(ValueError, match="ne suffisent pas"):
        ref.generate_branches(n=5)


def test_generation_reproductible_a_graine_egale():
    premier = ref.generate_all({"customers": 500, "accounts": 800}, seed=1)
    second = ref.generate_all({"customers": 500, "accounts": 800}, seed=1)
    assert premier["customers"].equals(second["customers"])
    assert premier["accounts"].equals(second["accounts"])
