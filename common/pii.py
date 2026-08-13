"""Pseudonymisation et masquage des données personnelles.

Placé dans `common/` parce que deux exécutants distincts en dépendent : le
générateur (application Streamlit) et les jobs PySpark de la couche Silver.

Trois primitives, trois usages :

* `pseudonymize` — jeton déterministe et non réversible, calculé par HMAC-SHA256.
  Déterministe pour rester joignable d'une table à l'autre ; à clé, pour qu'un
  attaquant disposant du jeton et de la liste des identifiants possibles ne
  puisse pas retrouver la valeur d'origine par force brute, ce qu'un simple
  SHA-256 ne garantirait pas sur un espace de clés aussi petit.
* `mask_iban` / `mask_account_number` — masquage d'affichage, qui conserve les
  derniers caractères pour permettre à un agent de reconnaître un compte sans
  exposer la valeur complète.

La clé est lue dans l'environnement. La valeur de repli n'est utilisable qu'en
développement local : en production elle proviendrait d'un secret Kubernetes,
et une rotation de clé invaliderait les jetons existants — compromis assumé,
documenté dans le write-up.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import Final

import pandas as pd

_FALLBACK_KEY: Final[str] = "waba-dev-only-pii-key-do-not-use-in-production"
TOKEN_LENGTH: Final[int] = 32


def _configured_key() -> str | None:
    """Clé fournie par l'environnement, ou None si absente **ou vide**.

    La distinction est essentielle : `docker compose` transmet les variables
    déclarées sans valeur comme des chaînes vides, pas comme des variables
    absentes. Un `os.getenv(..., défaut)` renverrait donc la chaîne vide et
    pseudonymiserait avec une clé HMAC nulle, sans que rien ne le signale.
    """
    value = os.getenv("WABA_PII_KEY")
    return value if value else None


def _key() -> bytes:
    return (_configured_key() or _FALLBACK_KEY).encode("utf-8")


def is_using_fallback_key() -> bool:
    """Indique si la clé de repli de développement est active.

    Permet aux jobs et à l'interface d'émettre un avertissement explicite
    plutôt que de pseudonymiser silencieusement avec une clé publique.
    """
    return _configured_key() is None


def pseudonymize_value(value: str | None) -> str | None:
    """Jeton déterministe pour une valeur unique. Propage les valeurs nulles."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digest = hmac.new(_key(), str(value).encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()[:TOKEN_LENGTH]


def pseudonymize(values: pd.Series) -> pd.Series:
    """Pseudonymise une colonne entière.

    Les valeurs identiques produisent le même jeton, ce qui préserve les
    jointures et les comptages distincts sur la donnée pseudonymisée.
    """
    return values.map(pseudonymize_value)


def mask_iban(values: pd.Series) -> pd.Series:
    """Masque un IBAN en ne conservant que le pays et les 4 derniers caractères.

    Exemple : CI93CI1010010100000000000123 -> CI**********************0123
    """
    def _mask(iban: str | None) -> str | None:
        if iban is None or pd.isna(iban):
            return None
        iban = str(iban)
        if len(iban) <= 6:
            return "*" * len(iban)
        return f"{iban[:2]}{'*' * (len(iban) - 6)}{iban[-4:]}"

    return values.map(_mask)


def mask_account_number(values: pd.Series) -> pd.Series:
    """Masque un numéro de compte en ne conservant que les 4 derniers chiffres."""
    def _mask(number: str | None) -> str | None:
        if number is None or pd.isna(number):
            return None
        number = str(number)
        if len(number) <= 4:
            return "*" * len(number)
        return f"{'*' * (len(number) - 4)}{number[-4:]}"

    return values.map(_mask)


#: Colonnes considérées comme données personnelles dans l'ensemble de la
#: plateforme. Sert de source unique au tagging PII d'OpenMetadata (Level 4.3)
#: et aux règles de masquage des jobs Silver.
PII_COLUMNS: Final[frozenset[str]] = frozenset({
    "customer_id",
    "sender_id",
    "receiver_id",
    "account_id",
    "beneficiary_account",
    "loan_account_id",
    "account_number",
    "iban",
})
