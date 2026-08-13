"""Cohérence de la configuration métier."""

from __future__ import annotations

import pytest

from generator import config as cfg


def test_les_huit_pays_du_sujet_sont_declares():
    assert set(cfg.COUNTRY_CODES) == {"CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"}


def test_le_ghana_est_le_seul_pays_hors_zone_franc():
    hors_uemoa = [c for c, pays in cfg.COUNTRIES.items() if not pays.is_uemoa]
    assert hors_uemoa == ["GH"]
    assert cfg.CURRENCY_BY_COUNTRY["GH"] == "GHS"
    assert {cfg.CURRENCY_BY_COUNTRY[c] for c in cfg.UEMOA_CODES} == {"XOF"}


@pytest.mark.parametrize(
    ("entity", "attendu"),
    [
        ("MOBILE_MONEY", {"CI", "SN", "BF", "GH"}),
        ("MICROFINANCE", {"ML", "GN", "BF"}),
        ("BANK", set(cfg.COUNTRY_CODES)),
        ("INSURANCE", set(cfg.COUNTRY_CODES)),
    ],
)
def test_matrice_pays_entite_conforme_au_tableau_des_entites(entity, attendu):
    """Le script de référence de l'annexe A.8 ignore cette matrice ; pas nous."""
    assert set(cfg.ENTITY_COUNTRIES[entity]) == attendu


def test_les_poids_des_entites_sont_renormalises_par_pays():
    for country in cfg.COUNTRY_CODES:
        entities, weights = cfg.entity_weights(country)
        assert entities == cfg.entities_in(country)
        assert sum(weights) == pytest.approx(1.0)


def test_aucune_entite_proposee_hors_de_son_perimetre():
    assert "MOBILE_MONEY" not in cfg.entities_in("ML")
    assert "MICROFINANCE" not in cfg.entities_in("CI")


def test_les_poids_pays_forment_une_distribution():
    assert sum(cfg.COUNTRY_WEIGHTS.values()) == pytest.approx(1.0, abs=1e-3)
    assert set(cfg.COUNTRY_WEIGHTS) == set(cfg.COUNTRY_CODES)


def test_parite_fixe_du_franc_cfa():
    """Le XOF est arrimé à l'euro par une parité réglementaire, pas un cours."""
    assert cfg.XOF_PER_EUR == pytest.approx(655.957)
    assert cfg.to_eur(655_957, "XOF") == pytest.approx(1000.0)


def test_seuils_reglementaires_exprimes_en_devise_locale():
    assert cfg.AML_THRESHOLD["XOF"] == 1_000_000
    assert cfg.AML_THRESHOLD["GHS"] == 5_000
    assert cfg.AML_THRESHOLD["GHS"] < cfg.AML_THRESHOLD["XOF"]


def test_les_branches_vie_ne_couvrent_pas_le_ghana():
    """WABA Assurance Vie opère en UEMOA seulement ; l'IARD couvre les 8 pays."""
    assert "GH" not in cfg.PRODUCT_LINE_COUNTRIES["VIE"]
    assert "GH" not in cfg.PRODUCT_LINE_COUNTRIES["PREVOYANCE"]
    assert "GH" in cfg.PRODUCT_LINE_COUNTRIES["IARD_AUTO"]


def test_le_microcredit_est_reserve_aux_pays_de_la_microfinance():
    for loan_type in ("MICROCREDIT", "AGRICULTURAL"):
        assert set(cfg.LOAN_TYPE_COUNTRIES[loan_type]) == set(cfg.ENTITY_COUNTRIES["MICROFINANCE"])
