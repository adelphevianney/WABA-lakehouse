"""Spécifications des jeux de données ingérés.

Un unique objet `DatasetSpec` par jeu de données pilote quatre usages : le
schéma de lecture, la DDL de la table Iceberg, les règles de validation et la
clé du MERGE. Écrire huit jobs, un par table, aurait multiplié par huit les
occasions de laisser diverger le schéma déclaré et le schéma réellement écrit.

Les schémas reprennent l'annexe A de l'énoncé. Deux colonnes techniques sont
ajoutées à toutes les tables : `ingestion_timestamp` et `source_file`. Elles ne
sont pas décoratives — le premier sert de clé de partition temporelle aux
référentiels, qui n'ont pas d'horodatage métier, et le second est ce qui
permettra de remonter d'une ligne du lakehouse au fichier dont elle provient,
matière première du data lineage du Level 4.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from common import domain as dom

INGESTION_TIMESTAMP = "ingestion_timestamp"
SOURCE_FILE = "source_file"

#: Colonne technique où Spark dépose les lignes structurellement illisibles
#: (nombre de colonnes incorrect, guillemets non fermés).
CORRUPT_RECORD = "_corrupt_record"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = False


@dataclass(frozen=True)
class DatasetSpec:
    """Description complète d'un jeu de données de la couche brute."""

    name: str                       # nom logique, présent dans le nom de fichier
    table: str                      # table Iceberg cible dans le schéma raw
    key: str                        # clé naturelle, support de l'idempotence
    columns: Tuple[Column, ...]
    country_column: str = "country_code"
    currency_column: Optional[str] = "currency"
    event_time: str = "timestamp"   # colonne portant la partition temporelle
    enums: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    non_negative: Tuple[str, ...] = ()
    #: Colonnes soumises à la matrice pays x entité. Vide pour les tables qui
    #: n'ont pas de sens géographique par entité.
    check_entity_matrix: bool = True

    @property
    def is_referential(self) -> bool:
        return self.event_time == INGESTION_TIMESTAMP

    @property
    def all_columns(self) -> Tuple[Column, ...]:
        """Colonnes métier suivies des colonnes de traçabilité."""
        return self.columns + (
            Column(INGESTION_TIMESTAMP, "TIMESTAMP", required=True),
            Column(SOURCE_FILE, "STRING", required=True),
        )

    @property
    def partitioning(self) -> str:
        """Partitionnement Iceberg : par pays et par date, comme exigé au §1.3.

        Les référentiels n'ayant pas d'horodatage métier, ils sont partitionnés
        sur leur date d'ingestion — ce qui revient à dater le millésime du
        référentiel, et n'ajoute aucune partition tant qu'il n'est chargé qu'une
        fois par jour.
        """
        return f"country_code, days({self.event_time})"


_COMMON_ENTITY = {"entity_type": dom.ENTITY_TYPES}
_CURRENCY = {"currency": dom.CURRENCIES}


BANK_TRANSACTIONS = DatasetSpec(
    name="bank_txn",
    table="bank_transactions",
    key="transaction_id",
    columns=(
        Column("transaction_id", "STRING", required=True),
        Column("timestamp", "TIMESTAMP", required=True),
        Column("account_id", "STRING", required=True),
        Column("beneficiary_account", "STRING"),
        Column("branch_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("transaction_type", "STRING"),
        Column("amount", "DOUBLE"),
        Column("currency", "STRING"),
        Column("channel", "STRING"),
        Column("transaction_status", "STRING"),
        Column("fee_amount", "DOUBLE"),
        Column("entity_type", "STRING", required=True),
    ),
    enums={
        "transaction_type": dom.TRANSACTION_TYPES,
        "channel": dom.CHANNELS,
        "transaction_status": dom.TRANSACTION_STATUSES,
        **_COMMON_ENTITY, **_CURRENCY,
    },
    non_negative=("amount", "fee_amount"),
)

INSURANCE_OPERATIONS = DatasetSpec(
    name="insurance_ops",
    table="insurance_operations",
    key="operation_id",
    columns=(
        Column("operation_id", "STRING", required=True),
        Column("timestamp", "TIMESTAMP", required=True),
        Column("customer_id", "STRING", required=True),
        Column("account_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("operation_type", "STRING"),
        Column("product_line", "STRING"),
        Column("amount", "DOUBLE"),
        Column("currency", "STRING"),
        Column("claim_status", "STRING"),
        Column("processing_days", "INT"),
        Column("entity_type", "STRING", required=True),
    ),
    enums={
        "operation_type": dom.INSURANCE_OPERATIONS,
        "product_line": dom.PRODUCT_LINES,
        "claim_status": dom.CLAIM_STATUSES,
        **_COMMON_ENTITY, **_CURRENCY,
    },
    non_negative=("amount", "processing_days"),
)

MOBILE_MONEY_PAYMENTS = DatasetSpec(
    name="mobile_money",
    table="mobile_money_payments",
    key="payment_id",
    columns=(
        Column("payment_id", "STRING", required=True),
        Column("timestamp", "TIMESTAMP", required=True),
        Column("sender_id", "STRING", required=True),
        Column("receiver_id", "STRING", required=True),
        Column("sender_country", "STRING", required=True),
        Column("receiver_country", "STRING", required=True),
        Column("amount", "DOUBLE"),
        Column("currency", "STRING"),
        Column("payment_type", "STRING"),
        Column("operator", "STRING"),
        Column("status", "STRING"),
        Column("fee_amount", "DOUBLE"),
        Column("entity_type", "STRING", required=True),
        Column("country_code", "STRING", required=True),
    ),
    enums={
        "payment_type": dom.PAYMENT_TYPES,
        "operator": dom.MOBILE_OPERATORS,
        "status": dom.MOBILE_STATUSES,
        **_COMMON_ENTITY, **_CURRENCY,
    },
    non_negative=("amount", "fee_amount"),
)

LOAN_REPAYMENTS = DatasetSpec(
    name="loan_repayments",
    table="loan_repayments",
    key="repayment_id",
    columns=(
        Column("repayment_id", "STRING", required=True),
        Column("timestamp", "TIMESTAMP", required=True),
        Column("loan_account_id", "STRING", required=True),
        Column("customer_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("amount_due", "DOUBLE"),
        Column("amount_paid", "DOUBLE"),
        # Ajoutée au schéma A.7, qui ne distingue pas capital et intérêts dans
        # le montant remboursé : sans cette part, le revenu par client attendu
        # au §2.3 — « commissions + intérêts perçus » — reste incalculable.
        Column("interest_amount", "DOUBLE"),
        Column("currency", "STRING"),
        Column("due_date", "DATE"),
        Column("payment_date", "DATE"),
        Column("days_overdue", "INT"),
        Column("loan_type", "STRING"),
        Column("repayment_status", "STRING"),
        Column("entity_type", "STRING", required=True),
    ),
    enums={
        "loan_type": dom.LOAN_TYPES,
        "repayment_status": dom.REPAYMENT_STATUSES,
        **_COMMON_ENTITY, **_CURRENCY,
    },
    non_negative=("amount_due", "amount_paid", "interest_amount", "days_overdue"),
)

CUSTOMERS = DatasetSpec(
    name="customers",
    table="customers",
    key="customer_id",
    columns=(
        Column("customer_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("entity_type", "STRING", required=True),
        Column("segment", "STRING"),
        Column("kyc_level", "STRING"),
        Column("onboarding_date", "DATE"),
        Column("region", "STRING"),
        Column("is_active", "BOOLEAN"),
    ),
    currency_column=None,
    event_time=INGESTION_TIMESTAMP,
    enums={"segment": dom.SEGMENTS, "kyc_level": dom.KYC_LEVELS, **_COMMON_ENTITY},
)

ACCOUNTS = DatasetSpec(
    name="accounts",
    table="accounts",
    key="account_id",
    columns=(
        Column("account_id", "STRING", required=True),
        Column("customer_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("entity_type", "STRING", required=True),
        Column("account_type", "STRING"),
        Column("account_number", "STRING"),
        # Nul au Ghana, qui ne fait pas partie du registre IBAN : c'est une
        # réalité métier, pas une donnée manquante.
        Column("iban", "STRING"),
        Column("currency", "STRING"),
        Column("balance", "DOUBLE"),
        Column("credit_limit", "DOUBLE"),
        Column("opened_date", "DATE"),
        Column("status", "STRING"),
        # Classification prudentielle portée par le contrat, nulle hors prêt.
        # Ajoutée au schéma A.2 : sans elle, le NPL ne serait calculable qu'à
        # partir des échéances observées, donc sur une fraction du portefeuille.
        Column("days_past_due", "INT"),
    ),
    event_time=INGESTION_TIMESTAMP,
    enums={
        "account_type": dom.ACCOUNT_TYPES,
        "status": dom.ACCOUNT_STATUSES,
        **_COMMON_ENTITY, **_CURRENCY,
    },
    # Le solde d'un compte courant peut être débiteur ; le plafond de crédit non.
    non_negative=("credit_limit",),
)

BRANCHES = DatasetSpec(
    name="branches",
    table="branches",
    key="branch_id",
    columns=(
        Column("branch_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("entity_type", "STRING", required=True),
        Column("city", "STRING"),
        Column("region", "STRING"),
        Column("branch_type", "STRING"),
        Column("is_active", "BOOLEAN"),
    ),
    currency_column=None,
    event_time=INGESTION_TIMESTAMP,
    enums={"branch_type": dom.BRANCH_TYPES, **_COMMON_ENTITY},
)

PRODUCTS = DatasetSpec(
    name="products",
    table="products",
    key="product_id",
    columns=(
        Column("product_id", "STRING", required=True),
        Column("country_code", "STRING", required=True),
        Column("entity_type", "STRING", required=True),
        Column("category", "STRING"),
        Column("product_name", "STRING"),
        Column("currency", "STRING"),
        Column("launch_date", "DATE"),
        Column("is_active", "BOOLEAN"),
    ),
    event_time=INGESTION_TIMESTAMP,
    enums={**_COMMON_ENTITY, **_CURRENCY},
)


#: Les huit jeux de données de la couche brute, dans l'ordre de dépendance :
#: les référentiels d'abord, comme l'exige l'annexe A.8.
SPECS: Tuple[DatasetSpec, ...] = (
    CUSTOMERS, ACCOUNTS, BRANCHES, PRODUCTS,
    BANK_TRANSACTIONS, INSURANCE_OPERATIONS, MOBILE_MONEY_PAYMENTS, LOAN_REPAYMENTS,
)

BY_NAME: Dict[str, DatasetSpec] = {spec.name: spec for spec in SPECS}


#: Table recueillant les lignes écartées. Elles ne sont pas perdues : les
#: conserver avec leur motif de rejet est ce qui distingue une validation d'une
#: suppression silencieuse.
REJECTS_TABLE = "ingestion_rejects"

REJECTS_DDL_COLUMNS = (
    ("dataset", "STRING"),
    ("source_file", "STRING"),
    ("ingestion_timestamp", "TIMESTAMP"),
    ("reject_reason", "STRING"),
    ("raw_record", "STRING"),
)


def create_table_ddl(spec: DatasetSpec, table_identifier: str) -> str:
    """DDL de création de la table Iceberg cible."""
    columns = ",\n    ".join(
        f"{column.name} {column.type}" for column in spec.all_columns
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {table_identifier} (\n    {columns}\n)\n"
        f"USING iceberg\n"
        f"PARTITIONED BY ({spec.partitioning})\n"
        # La version 2 du format est requise par MERGE INTO, qui s'appuie sur
        # les suppressions au niveau ligne.
        f"TBLPROPERTIES ('format-version' = '2', "
        f"'write.parquet.compression-codec' = 'zstd')"
    )


def create_rejects_ddl(table_identifier: str) -> str:
    columns = ",\n    ".join(f"{name} {type_}" for name, type_ in REJECTS_DDL_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_identifier} (\n    {columns}\n)\n"
        f"USING iceberg\nPARTITIONED BY (dataset, days(ingestion_timestamp))\n"
        f"TBLPROPERTIES ('format-version' = '2')"
    )
