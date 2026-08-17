"""Transformations Bronze vers Silver (§2.2).

Silver est la couche où la donnée devient exploitable : dédupliquée, convertie
dans une devise commune, enrichie par les référentiels, et débarrassée de ses
valeurs aberrantes. Gold n'agrège plus ensuite que des colonnes déjà propres.

Quatre traitements structurent chaque table, et correspondent aux critères
d'évaluation du niveau :

* **déduplication** sur la clé naturelle, avec un ordre de départage imposé pour
  qu'un recalcul retienne toujours la même ligne ;
* **conversion en euros**, sans laquelle aucun agrégat multi-pays n'a de sens :
  franc CFA et cedi diffèrent d'un facteur 37 ;
* **jointure avec les référentiels**, qui apporte le segment du client, la nature
  du compte et l'agence d'origine — informations que les fichiers transactionnels
  ne portent pas ;
* **traitement explicite des nulls et des cas aberrants**, chaque règle produisant
  une colonne interprétable plutôt qu'une suppression silencieuse.

Les champs personnels sont masqués ici et non en amont : la couche brute conserve
la fidélité de la source, comme toute zone d'atterrissage, et c'est à partir de
Silver que la donnée est diffusée.

Chaque constructeur accepte une source explicite. Par défaut il lit la table
`raw.*` correspondante — c'est le chemin batch. Le job de streaming du Level 3
lui passe le micro-lot qu'il vient de consommer dans Kafka, et obtient des
colonnes strictement identiques. Sans cela, deux définitions de Silver
coexisteraient et divergeraient au premier ajustement de règle métier, alors même
que les deux chemins alimentent les mêmes tables.

Exemples :
    python -m jobs.batch.silver
    python -m jobs.batch.silver --tables bank_transactions --countries CI SN
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import domain as dom
from jobs.batch import layers
from jobs.batch.session import build_session

logger = logging.getLogger("jobs.silver")

#: Au-delà de ce ratio, des frais rapportés au montant sont considérés comme
#: aberrants. Le barème du groupe est de 0,1 % ; 5 % relève de l'anomalie de
#: saisie ou du bug de facturation, pas de la tarification.
MAX_FEE_RATIO = 0.05


@dataclass
class SilverOutcome:
    table: str
    rows_in: int = 0
    rows_out: int = 0
    rows_added: int = 0
    duplicates: int = 0
    outliers: int = 0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _restrict(frame: DataFrame, countries: Optional[List[str]]) -> DataFrame:
    """Filtre par pays, pour permettre un retraitement sélectif depuis Airflow."""
    if not countries:
        return frame
    return frame.filter(F.col("country_code").isin(countries))


def _with_processed_at(frame: DataFrame) -> DataFrame:
    return frame.withColumn(layers.PROCESSED_AT, F.current_timestamp())


# =============================================================================
# Référentiels
# =============================================================================


def build_customers(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "customers")
    source = _restrict(_source, countries)
    deduplicated = layers.deduplicate(source, "customer_id", ["customer_id"])

    return _with_processed_at(
        deduplicated.select(
            F.col("customer_id"),
            # Jeton réutilisable là où l'analyse n'a pas besoin de l'identité.
            layers.token(F.col("customer_id")).alias("customer_token"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("segment"),
            F.col("kyc_level"),
            F.col("onboarding_date"),
            F.col("region"),
            F.col("is_active"),
            # L'ancienneté est une dimension d'analyse courante, que l'on
            # calcule une fois ici plutôt que dans chaque requête aval.
            F.months_between(F.current_date(), F.col("onboarding_date")).cast("int")
             .alias("anciennete_mois"),
            # Un client au KYC sommaire ne peut pas être servi comme un autre :
            # l'information conditionne les plafonds réglementaires.
            (F.col("kyc_level") == F.lit("BASIC")).alias("kyc_limite"),
        )
    )


def build_accounts(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "accounts")
    source = _restrict(_source, countries)
    deduplicated = layers.deduplicate(source, "account_id", ["account_id"])
    customers = layers.read(spark, "customers").select(
        F.col("customer_id"), F.col("segment").alias("customer_segment"),
        F.col("kyc_level").alias("customer_kyc"),
    )

    enriched = deduplicated.join(customers, on="customer_id", how="left")

    return _with_processed_at(
        enriched.select(
            F.col("account_id"),
            F.col("customer_id"),
            layers.token(F.col("customer_id")).alias("customer_token"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("account_type"),
            # Champs personnels masqués : seuls les derniers caractères restent
            # lisibles, assez pour reconnaître un compte sans l'exposer.
            layers.mask_tail("account_number", keep=4).alias("account_number_masked"),
            # L'IBAN est nul au Ghana, hors du registre IBAN. Le masque propage
            # ce null plutôt que d'inventer une valeur.
            layers.mask_tail("iban", keep=4, prefix=2).alias("iban_masked"),
            F.col("currency"),
            F.col("balance"),
            layers.to_eur(F.col("balance"), F.col("currency")).alias("balance_eur"),
            layers.to_eur(F.col("credit_limit"), F.col("currency")).alias("credit_limit_eur"),
            F.col("opened_date"),
            F.col("status"),
            F.col("customer_segment"),
            F.col("customer_kyc"),
            F.col("days_past_due"),
            # Classification prudentielle : au-delà de 90 jours d'impayé, la
            # créance est douteuse. Portée par le contrat, elle rend le NPL
            # calculable sur l'intégralité du portefeuille.
            (F.col("days_past_due") > F.lit(dom.NPL_OVERDUE_DAYS)).alias("est_douteux"),
            (F.col("account_type") == F.lit("LOAN")).alias("est_pret"),
            # Un compte clos ou dormant fausse les moyennes d'encours : le
            # signaler permet de l'exclure explicitement en aval.
            F.col("status").isin("ACTIVE", "FROZEN").alias("est_ouvert"),
        )
    )


# =============================================================================
# Flux transactionnels
# =============================================================================


def build_bank_transactions(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "bank_transactions")
    source = _restrict(_source, countries)
    # Départage par date d'ingestion : en cas de doublon, la version la plus
    # récemment reçue fait foi.
    deduplicated = layers.deduplicate(
        source, "transaction_id", ["ingestion_timestamp", "source_file"]
    )

    accounts = layers.read(spark, "accounts").select(
        F.col("account_id"),
        F.col("customer_id").alias("customer_id"),
        F.col("account_type").alias("account_type"),
    )
    beneficiaires = layers.read(spark, "accounts").select(
        F.col("account_id").alias("beneficiary_account"),
        F.col("country_code").alias("beneficiary_country"),
    )
    customers = layers.read(spark, "customers").select(
        F.col("customer_id"), F.col("segment").alias("customer_segment"),
    )
    branches = layers.read(spark, "branches").select(
        F.col("branch_id"), F.col("city").alias("branch_city"),
        F.col("branch_type"),
    )

    enriched = (
        deduplicated
        .join(accounts, on="account_id", how="left")
        .join(beneficiaires, on="beneficiary_account", how="left")
        .join(customers, on="customer_id", how="left")
        .join(branches, on="branch_id", how="left")
    )

    amount_eur = layers.to_eur(F.col("amount"), F.col("currency"))
    seuil_aml = F.create_map(
        *[x for code, value in dom.AML_THRESHOLD.items()
          for x in (F.lit(code), F.lit(float(value)))]
    )[F.col("currency")]

    return _with_processed_at(
        enriched.select(
            F.col("transaction_id"),
            F.col("timestamp"),
            F.to_date(F.col("timestamp")).alias("transaction_date"),
            F.col("account_id"),
            F.col("customer_id"),
            layers.token(F.col("customer_id")).alias("customer_token"),
            F.col("customer_segment"),
            F.col("account_type"),
            F.col("beneficiary_account"),
            F.col("beneficiary_country"),
            F.col("branch_id"),
            F.col("branch_city"),
            F.col("branch_type"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("transaction_type"),
            F.col("channel"),
            F.col("transaction_status"),
            F.col("currency"),
            F.col("amount"),
            amount_eur.alias("amount_eur"),
            layers.to_eur(F.col("fee_amount"), F.col("currency")).alias("fee_amount_eur"),
            (F.col("transaction_status") == F.lit("SUCCESS")).alias("est_aboutie"),
            # Matière première de la surveillance AML du Level 3 : le seuil est
            # exprimé en devise locale, donc évalué avant conversion.
            (F.col("transaction_type").isin(list(dom.WIRE_TRANSACTION_TYPES))
             & (F.col("amount") > seuil_aml)).alias("depasse_seuil_aml"),
            # Un virement dont le bénéficiaire est dans un autre pays alimente
            # l'analyse des flux transfrontaliers.
            (F.col("beneficiary_country").isNotNull()
             & (F.col("beneficiary_country") != F.col("country_code"))).alias("est_transfrontalier"),
            # Cas aberrant : des frais disproportionnés au montant trahissent une
            # erreur de facturation, pas une tarification.
            ((F.col("amount") > 0) & (F.col("fee_amount") / F.col("amount") > MAX_FEE_RATIO))
            .alias("frais_aberrants"),
            # Null métier : un compte bénéficiaire absent du référentiel signale
            # un virement sortant du périmètre du groupe.
            F.col("beneficiary_country").isNull().alias("beneficiaire_hors_groupe"),
            F.col("source_file"),
            F.col("ingestion_timestamp"),
        )
    )


def build_insurance_operations(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "insurance_operations")
    source = _restrict(_source, countries)
    deduplicated = layers.deduplicate(
        source, "operation_id", ["ingestion_timestamp", "source_file"]
    )
    customers = layers.read(spark, "customers").select(
        F.col("customer_id"), F.col("segment").alias("customer_segment"),
        F.col("kyc_level").alias("customer_kyc"),
    )

    enriched = deduplicated.join(customers, on="customer_id", how="left")
    est_sinistre = F.col("operation_type").isin(list(dom.CLAIM_OPERATIONS))

    return _with_processed_at(
        enriched.select(
            F.col("operation_id"),
            F.col("timestamp"),
            F.to_date(F.col("timestamp")).alias("operation_date"),
            F.trunc(F.col("timestamp"), "month").alias("operation_month"),
            F.col("customer_id"),
            layers.token(F.col("customer_id")).alias("customer_token"),
            F.col("customer_segment"),
            F.col("customer_kyc"),
            F.col("account_id"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("operation_type"),
            F.col("product_line"),
            F.col("currency"),
            F.col("amount"),
            layers.to_eur(F.col("amount"), F.col("currency")).alias("amount_eur"),
            F.col("claim_status"),
            # Le délai de traitement n'existe que pour un sinistre : le null des
            # autres opérations est porteur de sens et doit être conservé tel quel.
            F.col("processing_days"),
            est_sinistre.alias("est_sinistre"),
            F.col("operation_type").isin(list(dom.PREMIUM_OPERATIONS)).alias("est_prime"),
            (F.col("operation_type") == F.lit("CLAIM_PAYMENT")).alias("est_sinistre_regle"),
            # Cas aberrant : un sinistre sans délai de traitement renseigné, ou
            # une opération non sinistre qui en porte un.
            (est_sinistre & F.col("processing_days").isNull()).alias("delai_manquant"),
            F.col("source_file"),
            F.col("ingestion_timestamp"),
        )
    )


def build_mobile_money_payments(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "mobile_money_payments")
    source = _restrict(_source, countries)
    deduplicated = layers.deduplicate(
        source, "payment_id", ["ingestion_timestamp", "source_file"]
    )
    emetteurs = layers.read(spark, "customers").select(
        F.col("customer_id").alias("sender_id"),
        F.col("country_code").alias("sender_home_country"),
        F.col("segment").alias("sender_segment"),
    )

    enriched = deduplicated.join(emetteurs, on="sender_id", how="left")

    return _with_processed_at(
        enriched.select(
            F.col("payment_id"),
            F.col("timestamp"),
            F.to_date(F.col("timestamp")).alias("payment_date"),
            F.hour(F.col("timestamp")).alias("payment_hour"),
            F.col("sender_id"),
            layers.token(F.col("sender_id")).alias("sender_token"),
            F.col("receiver_id"),
            layers.token(F.col("receiver_id")).alias("receiver_token"),
            F.col("sender_segment"),
            F.col("sender_country"),
            F.col("receiver_country"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("payment_type"),
            F.col("operator"),
            F.col("status"),
            F.col("currency"),
            F.col("amount"),
            layers.to_eur(F.col("amount"), F.col("currency")).alias("amount_eur"),
            layers.to_eur(F.col("fee_amount"), F.col("currency")).alias("fee_amount_eur"),
            (F.col("status") == F.lit("SUCCESS")).alias("est_aboutie"),
            (F.col("sender_country") != F.col("receiver_country")).alias("est_transfrontalier"),
            # Corridor normalisé, sur lequel s'appuiera l'analyse des flux.
            F.concat_ws("-", F.col("sender_country"), F.col("receiver_country")).alias("corridor"),
            # Émission depuis un pays autre que celui de rattachement du client :
            # c'est le motif que la règle de fraude du Level 3 doit repérer.
            (F.col("sender_home_country").isNotNull()
             & (F.col("sender_home_country") != F.col("sender_country")))
            .alias("origine_inhabituelle"),
            F.col("source_file"),
            F.col("ingestion_timestamp"),
        )
    )


def build_loan_repayments(
    spark: SparkSession,
    countries: Optional[List[str]],
    source: Optional[DataFrame] = None,
) -> DataFrame:
    _source = source if source is not None else layers.read(spark, "loan_repayments")
    source = _restrict(_source, countries)
    deduplicated = layers.deduplicate(
        source, "repayment_id", ["ingestion_timestamp", "source_file"]
    )
    comptes = layers.read(spark, "accounts").select(
        F.col("account_id").alias("loan_account_id"),
        F.col("balance").alias("encours"),
        F.col("currency").alias("account_currency"),
    )

    enriched = deduplicated.join(comptes, on="loan_account_id", how="left")

    return _with_processed_at(
        enriched.select(
            F.col("repayment_id"),
            F.col("timestamp"),
            F.to_date(F.col("timestamp")).alias("repayment_date"),
            F.col("loan_account_id"),
            F.col("customer_id"),
            layers.token(F.col("customer_id")).alias("customer_token"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("loan_type"),
            F.col("currency"),
            F.col("amount_due"),
            F.col("amount_paid"),
            F.col("interest_amount"),
            layers.to_eur(F.col("amount_due"), F.col("currency")).alias("amount_due_eur"),
            layers.to_eur(F.col("amount_paid"), F.col("currency")).alias("amount_paid_eur"),
            # Revenu d'intérêt du groupe, composante du revenu par client.
            layers.to_eur(F.col("interest_amount"), F.col("currency")).alias("interest_amount_eur"),
            layers.to_eur(F.col("encours"), F.col("account_currency")).alias("encours_eur"),
            F.col("due_date"),
            # Null métier : une échéance impayée n'a pas de date de paiement.
            F.col("payment_date"),
            F.col("days_overdue"),
            F.col("repayment_status"),
            # Classification prudentielle : au-delà de 90 jours d'impayé, la
            # créance est douteuse. C'est cette colonne, et non le statut de
            # remboursement, qui fait foi pour le calcul du NPL.
            (F.col("days_overdue") > F.lit(dom.NPL_OVERDUE_DAYS)).alias("est_douteux"),
            (F.col("amount_paid") >= F.col("amount_due")).alias("echeance_soldee"),
            # Cas aberrant : un paiement supérieur à l'échéance attendue.
            (F.col("amount_paid") > F.col("amount_due") * F.lit(1.01)).alias("surpaiement"),
            F.col("source_file"),
            F.col("ingestion_timestamp"),
        )
    )


# =============================================================================
# Orchestration
# =============================================================================


@dataclass
class SilverTable:
    name: str
    builder: Callable[[SparkSession, Optional[List[str]]], DataFrame]
    key: str
    partitioning: str
    outlier_column: Optional[str] = None


#: Les référentiels précèdent les flux : ces derniers s'y joignent.
TABLES: List[SilverTable] = [
    SilverTable("customers", build_customers, "customer_id", "country_code"),
    SilverTable("accounts", build_accounts, "account_id", "country_code"),
    SilverTable("bank_transactions", build_bank_transactions, "transaction_id",
                "country_code, days(timestamp)", "frais_aberrants"),
    SilverTable("insurance_operations", build_insurance_operations, "operation_id",
                "country_code, days(timestamp)", "delai_manquant"),
    SilverTable("mobile_money_payments", build_mobile_money_payments, "payment_id",
                "country_code, days(timestamp)", "origine_inhabituelle"),
    SilverTable("loan_repayments", build_loan_repayments, "repayment_id",
                "country_code, days(timestamp)", "surpaiement"),
]

BY_NAME: Dict[str, SilverTable] = {table.name: table for table in TABLES}


def run(
    tables: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
) -> List[SilverOutcome]:
    selected = [BY_NAME[name] for name in tables] if tables else TABLES
    spark = build_session("waba-silver")

    outcomes: List[SilverOutcome] = []
    for table in selected:
        started = time.perf_counter()
        outcome = SilverOutcome(table=table.name)
        try:
            frame = table.builder(spark, countries).cache()
            outcome.rows_out = frame.count()
            outcome.rows_in = _restrict(
                layers.read(spark, table.name), countries
            ).count()
            outcome.duplicates = outcome.rows_in - outcome.rows_out
            if table.outlier_column:
                outcome.outliers = frame.filter(F.col(table.outlier_column)).count()

            outcome.rows_added = layers.merge(
                spark, frame, table.name, layers.SILVER_NAMESPACE,
                table.key, table.partitioning,
            )
            frame.unpersist()
        except Exception as exc:  # noqa: BLE001 — une table en échec n'arrête pas les autres
            logger.exception("échec de la transformation Silver de %s", table.name)
            outcome.error = str(exc)

        logger.info(
            "%s : %d lignes lues, %d écrites, %d doublon(s) écarté(s) en %.1f s",
            table.name, outcome.rows_in, outcome.rows_out, outcome.duplicates,
            time.perf_counter() - started,
        )
        outcomes.append(outcome)

    spark.stop()
    return outcomes


def _render(outcomes: List[SilverOutcome]) -> None:
    header = "{:<24} {:>10} {:>10} {:>10} {:>10} {:>10}".format(
        "table silver", "lues", "écrites", "doublons", "ajoutées", "aberrantes"
    )
    print("\n" + header)
    print("-" * len(header))
    for outcome in outcomes:
        if not outcome.ok:
            print("{:<24} ÉCHEC — {}".format(outcome.table, outcome.error))
            continue
        print("{:<24} {:>10,} {:>10,} {:>10,} {:>10,} {:>10,}".format(
            outcome.table, outcome.rows_in, outcome.rows_out,
            outcome.duplicates, outcome.rows_added, outcome.outliers,
        ))
    print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.batch.silver",
        description="Construit les tables silver.* depuis la couche brute.",
    )
    parser.add_argument("--tables", nargs="*", choices=sorted(BY_NAME),
                        help="tables à reconstruire (défaut : toutes)")
    parser.add_argument("--countries", nargs="*", metavar="CC",
                        help="restreindre à certains pays, pour un retraitement sélectif")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    outcomes = run(tables=args.tables, countries=args.countries)
    _render(outcomes)
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
