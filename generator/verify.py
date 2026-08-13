"""Vérification des critères d'évaluation du Level 1 liés au générateur.

Ne teste pas le code : relit ce qui se trouve réellement dans MinIO et contrôle
les propriétés que l'énoncé exige. C'est la différence entre « les tests
unitaires passent » et « les fichiers déposés sont conformes ».

Chaque contrôle correspond à une ligne de la grille d'évaluation du Level 1 :

* cohérence référentielle — aucune clé orpheline dans aucun fichier déposé ;
* multi-pays opérationnel — les fichiers sont partitionnés par pays et portent
  la colonne `country_code` ;
* cohérence métier — devises, matrice pays x entité, nomenclature des fichiers ;
* anomalies détectables — les trois règles de fraude du Level 3 trouvent bien
  de la matière dans les données générées.

Exécution :  docker compose exec streamlit python -m generator.verify
"""

from __future__ import annotations

import io
import re
import sys
from dataclasses import dataclass

import pandas as pd

from generator import anomalies as ano
from generator import config as cfg
from generator import storage as st

FILENAME_PATTERN = re.compile(
    r"^(?P<kind>[a-z_]+)_(?P<country>[A-Z]{2})_(?P<date>\d{8})_(?P<sequence>\d{2})\.csv$"
)


@dataclass
class Check:
    name: str
    passed: bool
    detail: str

    def render(self) -> str:
        mark = "\033[32m  OK  \033[0m" if self.passed else "\033[31m  KO  \033[0m"
        return f"{mark} {self.name:<46} {self.detail}"


def _read_csv(store: st.RawLandingStore, key: str) -> pd.DataFrame:
    response = store._client.get_object(Bucket=store.settings.raw_bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), dtype={"account_number": "string"})


def run() -> list[Check]:
    store = st.RawLandingStore()
    checks: list[Check] = []

    reachable, message = store.is_reachable()
    checks.append(Check("Connexion à MinIO", reachable, message))
    if not reachable:
        return checks

    # --- Référentiels --------------------------------------------------------
    present = store.referentials_present()
    checks.append(Check(
        "Les 4 référentiels sont déposés", present,
        "customers, accounts, branches, products" if present else "référentiels manquants",
    ))
    if not present:
        return checks

    referentials = store.load_referentials()
    customers, accounts = referentials["customers"], referentials["accounts"]
    branches = referentials["branches"]

    orphan_accounts = (~accounts["customer_id"].isin(set(customers["customer_id"]))).sum()
    checks.append(Check(
        "accounts → customers sans clé orpheline", orphan_accounts == 0,
        f"{len(accounts):,} comptes, {orphan_accounts} orphelins",
    ))

    # --- Fichiers transactionnels -------------------------------------------
    inventory = store.inventory()
    if inventory.empty:
        checks.append(Check("Fichiers de transactions déposés", False, "aucun fichier"))
        return checks

    countries = sorted(inventory["country_code"].unique())
    checks.append(Check(
        "Fichiers partitionnés par pays", len(countries) >= 1,
        f"{int(inventory['fichiers'].sum())} fichiers sur {len(countries)} pays : {', '.join(countries)}",
    ))

    keys = [k for k in store._list_keys("") if not k.startswith(st.REFERENTIALS_PREFIX)]
    bad_names = [k for k in keys if not FILENAME_PATTERN.match(k.split("/")[-1])]
    checks.append(Check(
        "Nomenclature <type>_<CC>_<AAAAMMJJ>_<NN>.csv", not bad_names,
        "conforme" if not bad_names else f"{len(bad_names)} fichiers hors nomenclature",
    ))

    misplaced = [k for k in keys if (p := k.split("/"))[0] != FILENAME_PATTERN.match(p[-1]).group("country")]
    checks.append(Check(
        "Arborescence pays/type respectée", not misplaced,
        "conforme" if not misplaced else f"{len(misplaced)} fichiers mal rangés",
    ))

    # --- Contrôles ligne à ligne, par type ----------------------------------
    account_ids = set(accounts["account_id"])
    customer_ids = set(customers["customer_id"])
    branch_ids = set(branches["branch_id"])
    universes = {
        "account_id": account_ids, "beneficiary_account": account_ids,
        "loan_account_id": account_ids, "branch_id": branch_ids,
        "customer_id": customer_ids, "sender_id": customer_ids, "receiver_id": customer_ids,
    }

    total_rows = total_orphans = 0
    currency_errors = matrix_errors = 0
    frames: dict[str, list[pd.DataFrame]] = {}

    for key in keys:
        kind = key.split("/")[1]
        frame = _read_csv(store, key)
        frames.setdefault(kind, []).append(frame)
        total_rows += len(frame)

        for column, universe in universes.items():
            if column in frame.columns:
                total_orphans += int((~frame[column].isin(universe)).sum())

        if "currency" in frame.columns and "country_code" in frame.columns:
            expected = frame["country_code"].map(cfg.CURRENCY_BY_COUNTRY)
            currency_errors += int((frame["currency"] != expected).sum())

        if "entity_type" in frame.columns and "country_code" in frame.columns:
            for entity, group in frame.groupby("entity_type"):
                allowed = set(cfg.countries_of(str(entity)))
                matrix_errors += int((~group["country_code"].isin(allowed)).sum())

    checks.append(Check(
        "Aucune clé orpheline dans les transactions", total_orphans == 0,
        f"{total_rows:,} lignes contrôlées, {total_orphans} clés orphelines",
    ))
    checks.append(Check(
        "Devise cohérente avec le pays", currency_errors == 0,
        f"{currency_errors} incohérences (XOF en zone UEMOA, GHS au Ghana)",
    ))
    checks.append(Check(
        "Matrice pays × entité respectée", matrix_errors == 0,
        f"{matrix_errors} lignes hors périmètre d'implantation du groupe",
    ))

    # --- Anomalies exploitables par le Level 3 -------------------------------
    if "bank_txn" in frames:
        bank = pd.concat(frames["bank_txn"], ignore_index=True)
        bank["timestamp"] = pd.to_datetime(bank["timestamp"])
        bursts = len(ano.detect_transaction_bursts(bank))
        checks.append(Check(
            "Règle 1 — rafales de virements détectables", bursts > 0,
            f"{bursts} transactions impliquées dans une rafale",
        ))
        aml = len(ano.detect_aml_events(bank))
        checks.append(Check(
            "AML — virements au-dessus du seuil déclaratif", aml > 0,
            f"{aml} virements ({aml / max(len(bank), 1):.1%} des transactions)",
        ))

    if "mobile_money" in frames:
        mobile = pd.concat(frames["mobile_money"], ignore_index=True)
        foreign = len(ano.detect_foreign_origin_payments(mobile, customers))
        checks.append(Check(
            "Règle 2 — paiements depuis un pays inhabituel", foreign > 0,
            f"{foreign} paiements émis par un client d'un autre pays",
        ))

    if "insurance_ops" in frames:
        insurance = pd.concat(frames["insurance_ops"], ignore_index=True)
        excessive = len(ano.detect_excessive_claims(insurance))
        checks.append(Check(
            "Règle 3 — sinistres disproportionnés", excessive > 0,
            f"{excessive} sinistres supérieurs à 3 fois la prime annuelle",
        ))

    return checks


def main() -> int:
    print("\n\033[36m=== Conformité des données déposées — Level 1 ===\033[0m\n")
    checks = run()
    for check in checks:
        print(check.render())

    failed = [c for c in checks if not c.passed]
    if failed:
        print(f"\n\033[31m{len(failed)} contrôle(s) en échec\033[0m\n")
        return 1
    print(f"\n\033[32mLes {len(checks)} contrôles passent\033[0m\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
