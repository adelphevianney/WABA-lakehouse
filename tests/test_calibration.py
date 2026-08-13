"""Calibration des indicateurs réglementaires."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from generator import calibration as calib
from generator import config as cfg


def test_cible_npl_dans_la_fourchette_exigee():
    for country in cfg.COUNTRY_CODES:
        cible = calib.npl_target(country)
        bas, haut = cfg.NPL_TARGET_RANGE
        assert bas < cible < haut


def test_cibles_npl_differenciees_entre_pays():
    """Une cible unique donnerait huit pastilles identiques au tableau de bord."""
    cibles = {calib.npl_target(c) for c in cfg.COUNTRY_CODES}
    assert len(cibles) == len(cfg.COUNTRY_CODES)


def test_cible_npl_stable_entre_deux_appels():
    assert calib.npl_target("CI") == calib.npl_target("CI")


def test_cible_loss_ratio_dans_la_fourchette_exigee():
    bas, haut = cfg.LOSS_RATIO_TARGET_RANGE
    for country in cfg.COUNTRY_CODES:
        for ligne in cfg.PRODUCT_LINES:
            assert bas <= calib.loss_ratio_target(country, ligne) <= haut


def test_certaines_branches_depassent_le_seuil_cima():
    """Sans franchissement, le tableau de bord Conformité n'a rien à signaler."""
    ratios = [
        calib.loss_ratio_target(pays, ligne)
        for pays in cfg.COUNTRY_CODES for ligne in cfg.PRODUCT_LINES
    ]
    assert any(r > cfg.LOSS_RATIO_ALERT for r in ratios)
    assert any(r < cfg.LOSS_RATIO_ALERT for r in ratios)


def test_defaut_deterministe_pour_un_meme_compte():
    comptes = pd.Series(["WABA-CI-A-0000001", "WABA-CI-A-0000002", "WABA-CI-A-0000003"])
    pays = pd.Series(["CI", "CI", "CI"])
    assert calib.is_defaulting_account(comptes, pays).equals(
        calib.is_defaulting_account(comptes, pays)
    )


def test_taux_de_defaut_proche_de_la_cible_du_pays():
    comptes = pd.Series([f"WABA-CI-A-{i:07d}" for i in range(1, 40_001)])
    pays = pd.Series(["CI"] * 40_000)
    observe = calib.is_defaulting_account(comptes, pays).mean()
    assert observe == pytest.approx(calib.npl_target("CI"), abs=0.005)


def test_jours_de_retard_coherents_avec_le_seuil_prudentiel():
    """Au-delà de 90 jours d'impayé, la créance est classée douteuse : le NPL
    doit être calculable indifféremment depuis le statut ou depuis le retard."""
    comptes = pd.Series([f"WABA-CI-A-{i:07d}" for i in range(1, 5_001)])
    defaillants = calib.is_defaulting_account(comptes, pd.Series(["CI"] * 5_000))
    retards = calib.overdue_days_for(comptes, defaillants)
    assert (retards[defaillants.to_numpy()] > 90).all()
    assert (retards[~defaillants.to_numpy()] <= 90).all()


def test_prime_annuelle_stable_par_police():
    polices = pd.Series(["WABA-CI-A-0000001", "WABA-GH-A-0000002"])
    assert np.array_equal(
        calib.annual_premium_eur(polices), calib.annual_premium_eur(polices)
    )


def test_sinistres_calibres_sur_le_loss_ratio_de_la_branche():
    rng = np.random.default_rng(0)
    primes = np.full(600, 100.0)
    lignes_primes = np.array(["IARD_AUTO"] * 600)
    lignes_sinistres = np.array(["IARD_AUTO"] * 120)

    montants = calib.claim_amounts_for_premiums(
        premium_amounts=primes,
        premium_lines=lignes_primes,
        claim_lines=lignes_sinistres,
        claim_annual_premiums=np.full(120, 1200.0),
        country_code="CI",
        rng=rng,
    )
    ratio = montants.sum() / primes.sum()
    assert ratio == pytest.approx(calib.loss_ratio_target("CI", "IARD_AUTO"), rel=1e-6)


def test_sinistres_sans_prime_dans_le_lot_restent_non_nuls():
    """Un lot peut ne contenir aucune prime sur une branche donnée : les
    sinistres de cette branche ne doivent pas s'effondrer à zéro."""
    montants = calib.claim_amounts_for_premiums(
        premium_amounts=np.full(10, 100.0),
        premium_lines=np.array(["VIE"] * 10),
        claim_lines=np.array(["IARD_AUTO"] * 5),
        claim_annual_premiums=np.full(5, 1200.0),
        country_code="CI",
        rng=np.random.default_rng(0),
    )
    assert (montants > 0).all()
