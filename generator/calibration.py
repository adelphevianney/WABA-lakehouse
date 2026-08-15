"""Calibration des indicateurs réglementaires.

L'énoncé impose que les KPIs Gold tombent dans des fourchettes réalistes :
NPL entre 3 % et 8 %, loss ratio entre 50 % et 85 %. Une génération purement
aléatoire ne peut pas garantir cela — le taux de défaut résulterait des poids
de `repayment_status`, et le loss ratio du rapport fortuit entre deux lois
lognormales indépendantes, typiquement plusieurs centaines de pour cent.

Ce module inverse la logique : on fixe d'abord la cible par pays, puis on
génère les données qui la produisent.

Toutes les décisions sont **déterministes et sans état**, dérivées par hachage
de l'identifiant concerné. Deux propriétés en découlent :

* rejouer la génération donne exactement les mêmes comptes en défaut, donc le
  même NPL — sans quoi chaque exécution ferait dériver l'indicateur ;
* aucun état partagé n'est nécessaire entre deux fichiers générés, ce qui
  permet de produire les fichiers pays par pays et jour par jour dans
  n'importe quel ordre.
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

from generator import config as cfg

# Fonction de mélange de SplitMix64. Un simple hachage multiplicatif laisse une
# structure résiduelle entre identifiants consécutifs, qui se traduit par des
# écarts systématiques sur le NPL pondéré par les encours. Les deux tours de
# xor-shift / multiplication de SplitMix64 suppriment cette structure pour un
# coût négligeable, là où un hachage cryptographique serait trop lent sur des
# colonnes de plusieurs centaines de milliers de lignes.
_MIX_A: Final[np.uint64] = np.uint64(0xBF58476D1CE4E5B9)
_MIX_B: Final[np.uint64] = np.uint64(0x94D049BB133111EB)
_GOLDEN: Final[np.uint64] = np.uint64(0x9E3779B97F4A7C15)
_TWO64: Final[float] = float(2**64)

_SALT_DEFAULT: Final[int] = 0x9E37
_SALT_OVERDUE: Final[int] = 0x51ED
_SALT_LOAN_TYPE: Final[int] = 0x7A3D


def _mix64(values: np.ndarray) -> np.ndarray:
    """Mélange SplitMix64, en arithmétique modulaire sur 64 bits."""
    x = values.astype(np.uint64)
    x = (x ^ (x >> np.uint64(30))) * _MIX_A
    x = (x ^ (x >> np.uint64(27))) * _MIX_B
    return x ^ (x >> np.uint64(31))


def _stable_uniform(keys: np.ndarray, salt: int) -> np.ndarray:
    """Tirage uniforme dans [0, 1), déterministe et vectorisé."""
    seeded = keys.astype(np.uint64) * _GOLDEN + np.uint64(salt)
    return _mix64(seeded).astype(np.float64) / _TWO64


def _sequence_of(identifiers: pd.Series, width: int) -> np.ndarray:
    """Extrait la séquence numérique d'un identifiant WABA-CC-X-0000123."""
    return identifiers.str[-width:].astype(np.int64).to_numpy()


def _target_from_name(name: str, low: float, high: float, salt: int) -> float:
    """Cible stable par pays ou par couple pays/produit."""
    seed = np.uint64(salt)
    for position, char in enumerate(name):
        seed = _mix64(np.array([seed + np.uint64(ord(char) * (position + 1))]))[0]
    return low + float(seed) / _TWO64 * (high - low)


# =============================================================================
# Créances douteuses (NPL)
# =============================================================================


def npl_target(country_code: str) -> float:
    """Taux de créances douteuses visé pour un pays.

    Chaque pays reçoit une cible stable dans la fourchette de l'énoncé, ce qui
    produit une dispersion crédible entre marchés : le tableau de bord Risque
    du Level 4 affiche des pastilles de couleur différentes, et le seuil BCEAO
    de 5 % est franchi par certains pays et pas par d'autres.
    """
    low, high = cfg.NPL_TARGET_RANGE
    margin = cfg.NPL_SAMPLING_MARGIN
    return _target_from_name(country_code, low + margin, high - margin, _SALT_DEFAULT)


def is_defaulting_account(account_ids: pd.Series, country_codes: pd.Series) -> pd.Series:
    """Marque les comptes de prêt en défaut pour atteindre la cible NPL du pays.

    La décision ne dépend que de l'identifiant du compte : un même compte est
    en défaut à chaque exécution, ce qui rend l'indicateur stable dans le temps.
    """
    draws = _stable_uniform(_sequence_of(account_ids, 7), _SALT_DEFAULT)
    targets = country_codes.map(npl_target).to_numpy()
    return pd.Series(draws < targets, index=account_ids.index)


def loan_type_index(account_ids: pd.Series, choices: int) -> np.ndarray:
    """Indice de type de prêt, stable pour un compte donné.

    Le type de prêt est une caractéristique du contrat, pas de chaque échéance :
    le tirer ligne par ligne ferait apparaître un même compte tantôt en
    microcrédit tantôt en crédit agricole, et rendrait incalculable le NPL
    ventilé par type de prêt.
    """
    draws = _stable_uniform(_sequence_of(account_ids, 7), _SALT_LOAN_TYPE)
    return np.minimum((draws * choices).astype(int), choices - 1)


def overdue_days_for(account_ids: pd.Series, defaulting: pd.Series) -> np.ndarray:
    """Jours de retard cohérents avec le statut du compte.

    La norme prudentielle classe une créance en douteuse au-delà de 90 jours
    d'impayé : les comptes en défaut dépassent ce seuil, les autres restent en
    deçà, ce qui rend le NPL calculable indifféremment depuis `repayment_status`
    ou depuis `days_overdue`.
    """
    draws = _stable_uniform(_sequence_of(account_ids, 7), _SALT_OVERDUE)
    in_default = 91 + (draws * 269).astype(int)      # 91 à 360 jours
    healthy = (draws * 45).astype(int)               # 0 à 44 jours
    return np.where(defaulting.to_numpy(), in_default, healthy)


# =============================================================================
# Ratio sinistres / primes (loss ratio)
# =============================================================================


def loss_ratio_target(country_code: str, product_line: str) -> float:
    """Ratio sinistres/primes visé pour un couple pays / branche d'assurance.

    Le seuil de vigilance CIMA est à 70 % : la fourchette de l'énoncé
    (50 % à 85 %) place volontairement certaines branches au-dessus, faute de
    quoi le tableau de bord Conformité n'aurait aucune alerte à afficher.
    """
    low, high = cfg.LOSS_RATIO_TARGET_RANGE
    return _target_from_name(f"{country_code}:{product_line}", low, high, _SALT_DEFAULT)


_SALT_PREMIUM: Final[int] = 0x2F1B


def annual_premium_eur(account_ids: pd.Series) -> np.ndarray:
    """Prime annuelle attachée à une police, en euros.

    Rattacher la prime à la police plutôt que de la tirer par opération est ce
    qui rend la règle de fraude « sinistre supérieur à trois fois la prime
    annuelle » exploitable : sans montant de référence stable par police, la
    règle n'a rien à comparer.

    Les versements de prime sont ensuite des échéances mensuelles de ce montant,
    et un sinistre en représente une fraction — un sinistre dépassant trois
    années de cotisation devient alors un vrai signal, et non le cas courant.
    """
    draws = _stable_uniform(_sequence_of(account_ids, 7), _SALT_PREMIUM)
    # Quantile d'une lognormale de médiane e^5.4 EUR (environ 220 EUR par an),
    # dispersée sur un facteur 10 entre les polices les plus et les moins chères.
    return np.exp(5.4 + 0.8 * _normal_quantile(draws))


def _normal_quantile(uniform: np.ndarray) -> np.ndarray:
    """Quantile normal centré réduit, par approximation de Beasley-Springer-Moro.

    Évite une dépendance à SciPy pour une transformation utilisée sur des
    colonnes de quelques milliers de lignes.
    """
    clipped = np.clip(uniform, 1e-12, 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00]

    result = np.empty_like(clipped)
    low, high = clipped < 0.02425, clipped > 1 - 0.02425
    central = ~(low | high)

    q = clipped[central] - 0.5
    r = q * q
    result[central] = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
                      (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)

    for mask, transform, sign in ((low, clipped, 1.0), (high, 1 - clipped, -1.0)):
        if mask.any():
            q = np.sqrt(-2 * np.log(transform[mask]))
            result[mask] = sign * (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
                           ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    return result


def claim_amounts_for_premiums(
    premium_amounts: np.ndarray,
    premium_lines: np.ndarray,
    claim_lines: np.ndarray,
    claim_annual_premiums: np.ndarray,
    country_code: str,
    rng: np.random.Generator,
) -> np.ndarray:
    """Dimensionne des sinistres pour atteindre le loss ratio visé, branche par branche.

    On part de l'enveloppe de primes réellement encaissée sur chaque branche, on
    en déduit la charge sinistres cible, puis on répartit cette charge sur les
    sinistres de la branche selon une loi lognormale. La dispersion individuelle
    reste réaliste, mais la somme respecte la cible — ce qu'un tirage
    indépendant des primes ne garantirait jamais.

    La granularité est celle du KPI `gold.loss_ratio_by_product` : calibrer
    globalement puis espérer que chaque branche tombe juste ne fonctionnerait pas.
    """
    amounts = np.zeros(len(claim_lines))
    if amounts.size == 0 or premium_amounts.size == 0:
        return amounts

    mean_premium = float(premium_amounts.mean())

    for line in np.unique(claim_lines):
        claim_mask = claim_lines == line
        n_claims = int(claim_mask.sum())

        collected = float(premium_amounts[premium_lines == line].sum())
        if collected == 0.0:
            # Aucune prime sur cette branche dans ce lot : on se rabat sur la
            # prime moyenne pour ne pas produire des sinistres à montant nul.
            collected = mean_premium * n_claims

        target = collected * loss_ratio_target(country_code, str(line))
        # Répartition proportionnelle à la prime de chaque police, bruitée : une
        # police chère porte des sinistres plus lourds, ce qu'une répartition
        # uniforme ne reproduirait pas.
        weights = claim_annual_premiums[claim_mask] * rng.lognormal(0.0, 0.55, n_claims)
        amounts[claim_mask] = target * weights / weights.sum()

    return amounts
