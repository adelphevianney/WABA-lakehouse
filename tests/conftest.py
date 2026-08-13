"""Fixtures partagées.

Les tests travaillent sur des référentiels réduits : les propriétés vérifiées
(intégrité référentielle, matrice pays x entité, cohérence des devises) sont
structurelles et ne dépendent pas de la volumétrie. Une suite qui générerait
800 000 comptes à chaque test ne serait plus exécutée.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

from generator import referentials as ref
from generator import transactions as txn

SEED = 20260813
PERIOD_START = datetime(2026, 1, 1)
PERIOD_END = datetime(2026, 3, 31, 23, 59, 59)


@pytest.fixture(scope="session")
def referentials() -> dict:
    return ref.generate_all({"customers": 6_000, "accounts": 10_000}, seed=SEED)


@pytest.fixture(scope="session")
def index(referentials) -> txn.ReferentialIndex:
    return txn.ReferentialIndex(**referentials)


@pytest.fixture
def rng() -> np.random.Generator:
    return np.random.default_rng(SEED)
