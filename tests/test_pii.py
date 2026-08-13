"""Pseudonymisation et masquage des données personnelles."""

from __future__ import annotations

import pandas as pd
import pytest

from common import pii


@pytest.fixture(autouse=True)
def _clef_de_test(monkeypatch):
    monkeypatch.setenv("WABA_PII_KEY", "clef-de-test")


def test_jeton_deterministe():
    """Le déterminisme est ce qui préserve les jointures et les COUNT DISTINCT
    sur la donnée pseudonymisée."""
    assert pii.pseudonymize_value("WABA-CI-C-000001") == pii.pseudonymize_value("WABA-CI-C-000001")


def test_valeurs_distinctes_donnent_des_jetons_distincts():
    valeurs = pd.Series([f"WABA-CI-C-{i:06d}" for i in range(500)])
    assert pii.pseudonymize(valeurs).nunique() == 500


def test_jeton_ne_contient_pas_la_valeur_source():
    jeton = pii.pseudonymize_value("WABA-CI-C-000001")
    assert "WABA" not in jeton and "000001" not in jeton
    assert len(jeton) == pii.TOKEN_LENGTH


def test_changer_de_cle_change_le_jeton(monkeypatch):
    avant = pii.pseudonymize_value("WABA-CI-C-000001")
    monkeypatch.setenv("WABA_PII_KEY", "une-autre-clef")
    assert pii.pseudonymize_value("WABA-CI-C-000001") != avant


def test_valeurs_nulles_propagees():
    resultat = pii.pseudonymize(pd.Series(["WABA-CI-C-000001", None]))
    assert resultat.iloc[1] is None


def test_cle_absente_bascule_sur_le_repli(monkeypatch):
    monkeypatch.delenv("WABA_PII_KEY", raising=False)
    assert pii.is_using_fallback_key()


def test_cle_vide_traitee_comme_absente(monkeypatch):
    """Docker Compose transmet une variable déclarée sans valeur comme une
    chaîne vide : sans ce traitement, le HMAC utiliserait une clé nulle et
    l'avertissement ne s'afficherait pas."""
    monkeypatch.setenv("WABA_PII_KEY", "")
    assert pii.is_using_fallback_key()
    # La clé de repli doit réellement être utilisée : un HMAC à clé vide
    # produirait un jeton, mais sans aucune valeur de protection.
    monkeypatch.delenv("WABA_PII_KEY")
    attendu = pii.pseudonymize_value("WABA-CI-C-000001")
    monkeypatch.setenv("WABA_PII_KEY", "")
    assert pii.pseudonymize_value("WABA-CI-C-000001") == attendu


def test_cle_renseignee_desactive_l_avertissement():
    assert not pii.is_using_fallback_key()


def test_masquage_iban_conserve_pays_et_quatre_derniers():
    masque = pii.mask_iban(pd.Series(["CI93CI1010010100000000000123"])).iloc[0]
    assert masque.startswith("CI") and masque.endswith("0123")
    assert "1010010100" not in masque


def test_masquage_iban_propage_les_nulls():
    """Les comptes ghanéens n'ont pas d'IBAN : le masquage ne doit pas inventer."""
    assert pii.mask_iban(pd.Series([None])).iloc[0] is None


def test_masquage_numero_de_compte():
    masque = pii.mask_account_number(pd.Series(["00000123456"])).iloc[0]
    assert masque == "*******3456"


def test_colonnes_sensibles_couvrent_les_champs_du_level_4():
    """Le Level 4 exige le tagging PII de customer_id, account_number et iban."""
    assert {"customer_id", "account_number", "iban"} <= pii.PII_COLUMNS
