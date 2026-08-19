"""Calcul des KPIs financiers et réglementaires (§2.3).

Sept tables, restituées à la maille attendue par les métiers : pays, entité,
segment, produit, corridor. Toutes les données sont déjà nettoyées et converties
en euros par la couche Silver ; Gold n'agrège plus que des colonnes fiables.

Deux indicateurs sont soumis à un seuil réglementaire et méritent une attention
particulière :

* `npl_ratio_by_country` est un **indicateur de stock**, calculé sur l'intégralité
  du portefeuille de prêts à une date, et non sur le flux des échéances d'une
  période. Un prêt se rembourse mensuellement : sur trois jours d'observation,
  seule une fraction du portefeuille produit une échéance, et rapporter les
  défauts observés au seul portefeuille observé sous-estime lourdement le ratio.
  Le dénominateur porte donc sur **tous** les comptes de prêt, et un prêt sans
  impayé constaté est réputé sain.
* `loss_ratio_by_product` rapporte les sinistres réglés aux primes acquises sur
  la même maille pays / branche / mois.

Exemples :
    python -m jobs.batch.gold
    python -m jobs.batch.gold --tables npl_ratio_by_country --countries CI
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import domain as dom
from jobs.batch import layers
from jobs.batch.session import build_session

logger = logging.getLogger("jobs.gold")


@dataclass
class GoldOutcome:
    table: str
    rows: int = 0
    rows_added: int = 0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


#: Fenêtre de recalcul, partagée avec la couche Silver : recalculer Gold sur un
#: horizon plus large que celui que Silver vient de reconstruire produirait des
#: agrégats mêlant deux millésimes de règles métier.
DEFAULT_WINDOW_DAYS: Optional[int] = int(os.getenv("WABA_MEDALLION_WINDOW_DAYS", "7")) or None

#: Colonne portant la date métier dans les tables Silver transactionnelles. Les
#: référentiels n'en ont pas : `restrict_window` les laisse alors intacts.
EVENT_TIME = "timestamp"

#: Fenêtre courante, posée par `run()` avant de construire les tables. Un
#: paramètre supplémentaire sur chacune des sept fonctions de construction
#: n'aurait rien apporté : elles lisent toutes Silver par `_silver`.
_WINDOW: Optional[int] = DEFAULT_WINDOW_DAYS


def _silver(spark: SparkSession, table: str, countries: Optional[List[str]]) -> DataFrame:
    frame = layers.restrict_window(
        layers.read(spark, table, layers.SILVER_NAMESPACE), _WINDOW, EVENT_TIME
    )
    if countries:
        frame = frame.filter(F.col("country_code").isin(countries))
    return frame


def _with_processed_at(frame: DataFrame) -> DataFrame:
    return frame.withColumn(layers.PROCESSED_AT, F.current_timestamp())


# =============================================================================
# KPIs bancaires
# =============================================================================


def daily_transaction_volume(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Volume et montant des transactions par jour, pays, entité et type."""
    transactions = _silver(spark, "bank_transactions", countries)

    return _with_processed_at(
        transactions.groupBy(
            "country_code", "transaction_date", "entity_type", "transaction_type"
        ).agg(
            F.count("*").alias("nb_transactions"),
            F.sum(F.col("est_aboutie").cast("int")).alias("nb_abouties"),
            F.round(F.sum("amount_eur"), 2).alias("montant_total_eur"),
            F.round(F.avg("amount_eur"), 2).alias("montant_moyen_eur"),
            F.round(F.sum("fee_amount_eur"), 2).alias("commissions_eur"),
            F.sum(F.col("depasse_seuil_aml").cast("int")).alias("nb_au_dessus_seuil_aml"),
        ).withColumn(
            "taux_echec",
            F.round(
                (F.col("nb_transactions") - F.col("nb_abouties")) * 100.0
                / F.col("nb_transactions"), 2
            ),
        )
    )


def npl_ratio_by_country(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Taux de créances douteuses par pays et par type de prêt.

    Le dénominateur est l'encours **de tous** les comptes de prêt, y compris ceux
    dont aucune échéance n'est tombée dans la période observée. C'est ce qui
    distingue un indicateur de stock d'un ratio de flux : ne retenir que les
    prêts vus récemment reviendrait à ignorer la majeure partie du portefeuille,
    puisqu'un prêt ne produit qu'une échéance par mois.

    Une ligne `ENSEMBLE` par pays porte l'indicateur réglementaire, comparé au
    seuil BCEAO. Les lignes par type de prêt en donnent la ventilation ; les
    prêts sans échéance enregistrée sur la période y figurent sous
    `NON_VENTILE`, faute de connaître leur type — mais ils comptent bien dans
    l'indicateur d'ensemble.
    """
    prets = _silver(spark, "accounts", countries).filter(F.col("est_pret"))

    # La classification vient du compte lui-même, et non des échéances observées.
    # C'est ce qui rend l'indicateur indépendant de la fenêtre de traitement :
    # un prêt ne produit qu'une échéance par mois, donc reconstruire son statut
    # à partir des seuls remboursements d'une période courte ne verrait qu'une
    # fraction du portefeuille et sous-estimerait le ratio d'autant.
    #
    # Le type de prêt, lui, n'existe que sur les échéances : il sert à ventiler,
    # pas à classer.
    types = _silver(spark, "loan_repayments", countries).groupBy(
        F.col("loan_account_id").alias("account_id"), F.col("country_code")
    ).agg(F.max("loan_type").alias("loan_type"))

    portefeuille = prets.join(types, on=["account_id", "country_code"], how="left").select(
        F.col("country_code"),
        F.coalesce(F.col("loan_type"), F.lit("NON_VENTILE")).alias("loan_type"),
        F.col("balance_eur").alias("encours_eur"),
        F.col("est_douteux"),
    )

    def agrege(frame: DataFrame, dimension) -> DataFrame:
        return frame.groupBy("country_code", dimension).agg(
            F.count("*").alias("nb_prets"),
            F.sum(F.col("est_douteux").cast("int")).alias("nb_prets_douteux"),
            F.round(F.sum("encours_eur"), 2).alias("encours_total_eur"),
            F.round(F.sum(F.when(F.col("est_douteux"), F.col("encours_eur"))
                          .otherwise(F.lit(0.0))), 2).alias("encours_douteux_eur"),
        )

    par_type = agrege(portefeuille, F.col("loan_type"))
    ensemble = agrege(portefeuille, F.lit("ENSEMBLE").alias("loan_type"))

    return _with_processed_at(
        par_type.unionByName(ensemble)
        .withColumn(
            "npl_ratio",
            F.round(F.col("encours_douteux_eur") / F.col("encours_total_eur"), 4),
        )
        .withColumn(
            "seuil_bceao_depasse",
            F.col("npl_ratio") > F.lit(dom.NPL_REGULATORY_CEILING),
        )
    )


def customer_arpu_monthly(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Revenu moyen par client, mensuel, par pays et segment.

    Le revenu retenu est celui que l'énoncé définit : commissions encaissées et
    intérêts perçus. Les commissions viennent des transactions bancaires et des
    paiements mobile money, les intérêts de la part d'intérêt des échéances de
    prêt — colonne ajoutée au schéma A.7, qui ne distingue pas capital et
    intérêts dans le montant remboursé.
    """
    clients = _silver(spark, "customers", countries).select(
        "customer_id", "country_code", "segment"
    )

    def mensualise(frame: DataFrame, date_col: str, montant: str) -> DataFrame:
        return frame.select(
            F.col("customer_id"),
            F.col("country_code"),
            F.trunc(F.col(date_col), "month").alias("mois"),
            F.col(montant).alias("revenu_eur"),
        )

    banque = mensualise(
        _silver(spark, "bank_transactions", countries).filter(F.col("est_aboutie")),
        "transaction_date", "fee_amount_eur",
    )
    mobile = mensualise(
        _silver(spark, "mobile_money_payments", countries)
        .filter(F.col("est_aboutie"))
        .withColumn("customer_id", F.col("sender_id")),
        "payment_date", "fee_amount_eur",
    )
    interets = mensualise(
        _silver(spark, "loan_repayments", countries),
        "repayment_date", "interest_amount_eur",
    )

    revenus = banque.unionByName(mobile).unionByName(interets)
    enrichi = revenus.join(clients.drop("country_code"), on="customer_id", how="left")

    return _with_processed_at(
        enrichi.groupBy("country_code", "mois", "segment").agg(
            F.countDistinct("customer_id").alias("clients_actifs"),
            F.round(F.sum("revenu_eur"), 2).alias("revenus_eur"),
        ).withColumn(
            "arpc_eur",
            F.round(F.col("revenus_eur") / F.col("clients_actifs"), 2),
        )
    )


# =============================================================================
# KPIs assurance
# =============================================================================


def loss_ratio_by_product(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Ratio sinistres sur primes par pays, branche et mois."""
    operations = _silver(spark, "insurance_operations", countries)

    return _with_processed_at(
        operations.groupBy("country_code", "product_line", "operation_month").agg(
            F.round(F.sum(F.when(F.col("est_prime"), F.col("amount_eur"))
                          .otherwise(F.lit(0.0))), 2).alias("primes_acquises_eur"),
            F.round(F.sum(F.when(F.col("est_sinistre_regle"), F.col("amount_eur"))
                          .otherwise(F.lit(0.0))), 2).alias("sinistres_regles_eur"),
            F.sum(F.col("est_sinistre_regle").cast("int")).alias("nb_sinistres_regles"),
        )
        .withColumnRenamed("operation_month", "mois")
        .withColumn(
            "loss_ratio",
            # Une branche sans prime encaissée sur le mois ne produit pas de
            # ratio : afficher zéro laisserait croire à une sinistralité nulle.
            F.when(F.col("primes_acquises_eur") > 0,
                   F.round(F.col("sinistres_regles_eur") / F.col("primes_acquises_eur"), 4)),
        )
        .withColumn(
            "seuil_cima_depasse",
            F.col("loss_ratio") > F.lit(dom.LOSS_RATIO_ALERT),
        )
    )


def claims_processing_time(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Délai moyen de traitement des sinistres, par pays et ligne IARD ou Vie."""
    sinistres = (
        _silver(spark, "insurance_operations", countries)
        .filter(F.col("est_sinistre") & F.col("processing_days").isNotNull())
    )

    ligne = F.when(
        F.col("product_line").isin("VIE", "PREVOYANCE"), F.lit("VIE")
    ).otherwise(F.lit("IARD"))

    return _with_processed_at(
        sinistres.withColumn("ligne_metier", ligne)
        .groupBy("country_code", "ligne_metier").agg(
            F.count("*").alias("nb_sinistres"),
            F.round(F.avg("processing_days"), 1).alias("delai_moyen_jours"),
            F.expr("percentile_approx(processing_days, 0.5)").alias("delai_median_jours"),
            # Le neuvième décile décrit la queue de distribution, celle qui
            # dégrade la satisfaction client sans peser sur la moyenne.
            F.expr("percentile_approx(processing_days, 0.9)").alias("delai_p90_jours"),
            F.max("processing_days").alias("delai_max_jours"),
        )
    )


# =============================================================================
# KPIs mobile money
# =============================================================================


def mobile_money_daily_flow(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Flux journalier de paiements mobiles par pays."""
    paiements = _silver(spark, "mobile_money_payments", countries)

    return _with_processed_at(
        paiements.groupBy("country_code", "payment_date").agg(
            F.count("*").alias("nb_paiements"),
            F.sum(F.col("est_aboutie").cast("int")).alias("nb_abouties"),
            F.round(F.sum("amount_eur"), 2).alias("montant_total_eur"),
            F.round(F.sum("fee_amount_eur"), 2).alias("commissions_eur"),
            # Utilisateurs actifs : émetteurs distincts du jour, indicateur
            # d'usage réel là où le volume seul peut venir de quelques comptes.
            F.countDistinct("sender_id").alias("utilisateurs_actifs"),
            F.sum(F.col("est_transfrontalier").cast("int")).alias("nb_transfrontaliers"),
        ).withColumn(
            "taux_echec",
            F.round((F.col("nb_paiements") - F.col("nb_abouties")) * 100.0
                    / F.col("nb_paiements"), 2),
        )
    )


def cross_border_transfers(spark: SparkSession, countries: Optional[List[str]]) -> DataFrame:
    """Flux transfrontaliers par corridor et par semaine, dans les deux sens.

    Chaque transfert est restitué deux fois : en sortant depuis le pays
    émetteur, en entrant depuis le pays destinataire. C'est ce qui permet à un
    responsable pays de lire ses flux dans les deux sens sans recomposer la
    symétrie lui-même.
    """
    transferts = (
        layers.read(spark, "mobile_money_payments", layers.SILVER_NAMESPACE)
        .filter(F.col("est_transfrontalier"))
        .withColumn("semaine", F.date_trunc("week", F.col("payment_date")).cast("date"))
    )

    sortants = transferts.select(
        F.col("sender_country").alias("country_code"),
        F.col("corridor"), F.col("semaine"), F.col("amount_eur"),
        F.lit("SORTANT").alias("sens"),
    )
    entrants = transferts.select(
        F.col("receiver_country").alias("country_code"),
        F.col("corridor"), F.col("semaine"), F.col("amount_eur"),
        F.lit("ENTRANT").alias("sens"),
    )

    flux = sortants.unionByName(entrants)
    if countries:
        flux = flux.filter(F.col("country_code").isin(countries))

    return _with_processed_at(
        flux.groupBy("country_code", "corridor", "sens", "semaine").agg(
            F.count("*").alias("nb_transferts"),
            F.round(F.sum("amount_eur"), 2).alias("montant_total_eur"),
            F.round(F.avg("amount_eur"), 2).alias("montant_moyen_eur"),
        )
    )


# =============================================================================
# Orchestration
# =============================================================================


@dataclass
class GoldTable:
    name: str
    builder: Callable[[SparkSession, Optional[List[str]]], DataFrame]
    keys: List[str]


#: Les sept tables du §2.3. Toutes sont partitionnées par `country_code` : ce
#: sont des agrégats de faible volume, qu'un partitionnement plus fin
#: morcellerait sans bénéfice.
TABLES: List[GoldTable] = [
    GoldTable("daily_transaction_volume", daily_transaction_volume,
              ["country_code", "transaction_date", "entity_type", "transaction_type"]),
    GoldTable("npl_ratio_by_country", npl_ratio_by_country,
              ["country_code", "loan_type"]),
    GoldTable("customer_arpu_monthly", customer_arpu_monthly,
              ["country_code", "mois", "segment"]),
    GoldTable("loss_ratio_by_product", loss_ratio_by_product,
              ["country_code", "product_line", "mois"]),
    GoldTable("claims_processing_time", claims_processing_time,
              ["country_code", "ligne_metier"]),
    GoldTable("mobile_money_daily_flow", mobile_money_daily_flow,
              ["country_code", "payment_date"]),
    GoldTable("cross_border_transfers", cross_border_transfers,
              ["country_code", "corridor", "sens", "semaine"]),
]

BY_NAME: Dict[str, GoldTable] = {table.name: table for table in TABLES}


def run(
    tables: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
    window_days: Optional[int] = DEFAULT_WINDOW_DAYS,
) -> List[GoldOutcome]:
    global _WINDOW
    _WINDOW = window_days
    selected = [BY_NAME[name] for name in tables] if tables else TABLES
    spark = build_session("waba-gold")

    outcomes: List[GoldOutcome] = []
    for table in selected:
        started = time.perf_counter()
        outcome = GoldOutcome(table=table.name)
        try:
            frame = table.builder(spark, countries).cache()
            outcome.rows = frame.count()
            outcome.rows_added = layers.merge(
                spark, frame, table.name, layers.GOLD_NAMESPACE,
                table.keys, "country_code",
            )
            frame.unpersist()
        except Exception as exc:  # noqa: BLE001 — un KPI en échec n'arrête pas les autres
            logger.exception("échec du calcul de %s", table.name)
            outcome.error = str(exc)

        logger.info("%s : %d lignes en %.1f s", table.name, outcome.rows,
                    time.perf_counter() - started)
        outcomes.append(outcome)

    spark.stop()
    return outcomes


def _render(outcomes: List[GoldOutcome]) -> None:
    header = "{:<28} {:>10} {:>10}".format("table gold", "lignes", "ajoutées")
    print("\n" + header)
    print("-" * len(header))
    for outcome in outcomes:
        if not outcome.ok:
            print("{:<28} ÉCHEC — {}".format(outcome.table, outcome.error))
            continue
        print("{:<28} {:>10,} {:>10,}".format(outcome.table, outcome.rows, outcome.rows_added))
    print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.batch.gold",
        description="Calcule les 7 tables de KPIs gold.* depuis la couche Silver.",
    )
    parser.add_argument("--tables", nargs="*", choices=sorted(BY_NAME),
                        help="KPIs à recalculer (défaut : tous)")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS,
                        metavar="N",
                        help="ne recalculer que les N derniers jours présents dans Silver "
                             "(défaut : %(default)s). 0 reconstruit tout.")
    parser.add_argument("--countries", nargs="*", metavar="CC",
                        help="restreindre à certains pays, pour un recalcul sélectif")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    outcomes = run(tables=args.tables, countries=args.countries,
                   window_days=args.window_days or None)
    _render(outcomes)
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
