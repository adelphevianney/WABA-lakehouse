"""Pseudonymisation et masquage des données personnelles.

Placé dans `common/` parce que deux exécutants distincts en dépendent : le
générateur (application Streamlit) et les jobs PySpark de la couche Silver.

Trois primitives, trois usages :

* `pseudonymize` — jeton déterministe et non réversible : SHA-256 d'une clé
  secrète concaténée à la valeur. Déterministe pour rester joignable d'une table
  à l'autre ; à clé, pour qu'un attaquant disposant du jeton et de la liste des
  identifiants possibles ne puisse pas retrouver la valeur d'origine par force
  brute, ce qu'un SHA-256 nu ne garantirait pas sur un espace aussi petit.

  Un HMAC serait le choix canonique, et sa supériorité tient à sa résistance à
  l'extension de longueur — une propriété qui protège l'authentification de
  messages, pas la pseudonymisation d'identifiants. Le choix d'un hachage à clé
  préfixée est ici dicté par une contrainte concrète : Spark n'expose pas de
  fonction HMAC native, et la seule alternative serait une UDF Python faisant
  transiter chaque ligne par l'interpréteur. Les jobs Silver reproduisent donc
  exactement cette construction avec `sha2(concat(clé, valeur), 256)`, garantissant
  des jetons identiques des deux côtés.
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
import os
from typing import TYPE_CHECKING, Final

# pandas n'est importé que par les fonctions qui manipulent des Series. Les
# primitives — clé et jeton — doivent rester utilisables depuis les jobs Spark,
# dont l'image n'embarque pas pandas : l'y ajouter coûterait 70 Mo pour une
# dépendance dont ces jobs n'ont aucun usage.
if TYPE_CHECKING:  # pragma: no cover
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
    return key_material().encode("utf-8")


def key_material() -> str:
    """Clé en clair, telle que les jobs Spark doivent l'injecter dans `concat`.

    Exposée pour que les transformations Silver reconstruisent exactement le même
    jeton sans dupliquer la logique de résolution de la clé.
    """
    return _configured_key() or _FALLBACK_KEY


def is_using_fallback_key() -> bool:
    """Indique si la clé de repli de développement est active.

    Permet aux jobs et à l'interface d'émettre un avertissement explicite
    plutôt que de pseudonymiser silencieusement avec une clé publique.
    """
    return _configured_key() is None


def pseudonymize_value(value: str | None) -> str | None:
    """Jeton déterministe pour une valeur unique. Propage les valeurs nulles.

    Construction volontairement identique à celle des jobs Spark :
    `sha2(concat(clé, valeur), 256)` tronqué. Toute divergence entre les deux
    produirait des jetons incompatibles et casserait silencieusement les
    jointures entre couches.
    """
    # `value != value` détecte un NaN sans dépendre de pandas.
    if value is None or (isinstance(value, float) and value != value):
        return None
    digest = hashlib.sha256(_key() + str(value).encode("utf-8"))
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
        if iban is None or iban != iban:
            return None
        iban = str(iban)
        if len(iban) <= 6:
            return "*" * len(iban)
        return f"{iban[:2]}{'*' * (len(iban) - 6)}{iban[-4:]}"

    return values.map(_mask)


def mask_account_number(values: pd.Series) -> pd.Series:
    """Masque un numéro de compte en ne conservant que les 4 derniers chiffres."""
    def _mask(number: str | None) -> str | None:
        if number is None or number != number:
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
