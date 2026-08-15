"""Agrégats réglementaires BCEAO et CIMA (§2.1).

Produit, pour une journée donnée et par pays, la synthèse que les autorités de
tutelle attendent : qualité du portefeuille de crédit pour la BCEAO, équilibre
technique de l'assurance pour la CIMA, et volumétrie des déclarations de
soupçon.

La table est datée de la **journée déclarée**, non de celle du calcul. Un
rapport recalculé une semaine plus tard doit remplacer celui de la journée
concernée, pas en créer un nouveau : c'est ce que garantit le MERGE sur le
couple date de rapport et pays.

Deux natures d'indicateurs cohabitent, et les confondre serait une erreur de
lecture :

* le **NPL est un stock**, photographié à la date du rapport sur l'intégralité
  du portefeuille de prêts, indépendamment de l'activité du jour ;
* les **volumes et déclarations sont des flux**, mesurés sur la seule journée.

Exemple :
    python -m jobs.batch.regulatory --report-date 2026-03-30
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import domain as dom
from jobs.batch import layers
from jobs.batch.session import build_session

logger = logging.getLogger("jobs.regulatory")

TABLE = "regulatory_report"


def _restrict(frame: DataFrame, countries: Optional[List[str]]) -> DataFrame:
    if not countries:
        return frame
    return frame.filter(F.col("country_code").isin(countries))


def build_report(
    spark: SparkSession, report_date: date, countries: Optional[List[str]]
) -> DataFrame:
    jour = F.lit(report_date.isoformat()).cast("date")

    # --- Volet BCEAO : qualité du portefeuille de crédit ---------------------
    # Indicateur de stock : l'encours douteux rapporté à l'encours total, sur
    # tout le portefeuille et non sur les seuls prêts mouvementés le jour même.
    npl = _restrict(
        layers.read(spark, "npl_ratio_by_country", layers.GOLD_NAMESPACE), countries
    ).filter(F.col("loan_type") == F.lit("ENSEMBLE")).select(
        F.col("country_code"),
        F.col("encours_total_eur"),
        F.col("encours_douteux_eur"),
        F.col("npl_ratio"),
        F.col("nb_prets"),
    )

    # --- Volet CIMA : équilibre technique de l'assurance ---------------------
    # Le ratio consolidé d'un pays se calcule sur les masses, non en moyennant
    # les ratios par branche : une branche marginale pèserait alors autant
    # qu'une branche majeure.
    assurance = _restrict(
        layers.read(spark, "loss_ratio_by_product", layers.GOLD_NAMESPACE), countries
    ).filter(F.trunc(jour, "month") == F.col("mois")).groupBy("country_code").agg(
        F.round(F.sum("primes_acquises_eur"), 2).alias("primes_acquises_eur"),
        F.round(F.sum("sinistres_regles_eur"), 2).alias("sinistres_regles_eur"),
    ).withColumn(
        "loss_ratio",
        F.when(F.col("primes_acquises_eur") > 0,
               F.round(F.col("sinistres_regles_eur") / F.col("primes_acquises_eur"), 4)),
    )

    # --- Volet déclaratif : opérations au-dessus du seuil --------------------
    transactions = _restrict(
        layers.read(spark, "bank_transactions", layers.SILVER_NAMESPACE), countries
    ).filter(F.col("transaction_date") == jour)

    activite = transactions.groupBy("country_code").agg(
        F.count("*").alias("nb_transactions"),
        F.round(F.sum("amount_eur"), 2).alias("montant_transactions_eur"),
        F.sum(F.col("depasse_seuil_aml").cast("int")).alias("nb_declarations_aml"),
        F.round(F.sum(F.when(F.col("depasse_seuil_aml"), F.col("amount_eur"))
                      .otherwise(F.lit(0.0))), 2).alias("montant_declare_aml_eur"),
    )

    return (
        npl.join(assurance, on="country_code", how="full_outer")
        .join(activite, on="country_code", how="full_outer")
        .select(
            jour.alias("reporting_date"),
            F.col("country_code"),
            # Volet BCEAO
            F.col("nb_prets"),
            F.col("encours_total_eur"),
            F.col("encours_douteux_eur"),
            F.col("npl_ratio"),
            (F.col("npl_ratio") <= F.lit(dom.NPL_REGULATORY_CEILING)).alias("npl_conforme"),
            F.lit(dom.NPL_REGULATORY_CEILING).alias("npl_seuil_bceao"),
            # Volet CIMA
            F.col("primes_acquises_eur"),
            F.col("sinistres_regles_eur"),
            F.col("loss_ratio"),
            (F.col("loss_ratio") <= F.lit(dom.LOSS_RATIO_ALERT)).alias("loss_ratio_conforme"),
            F.lit(dom.LOSS_RATIO_ALERT).alias("loss_ratio_seuil_cima"),
            # Volet déclaratif et activité
            F.coalesce(F.col("nb_transactions"), F.lit(0)).alias("nb_transactions"),
            F.coalesce(F.col("montant_transactions_eur"), F.lit(0.0)).alias("montant_transactions_eur"),
            F.coalesce(F.col("nb_declarations_aml"), F.lit(0)).alias("nb_declarations_aml"),
            F.coalesce(F.col("montant_declare_aml_eur"), F.lit(0.0)).alias("montant_declare_aml_eur"),
        )
        .withColumn(layers.PROCESSED_AT, F.current_timestamp())
    )


def run(report_date: date, countries: Optional[List[str]] = None) -> int:
    spark = build_session("waba-regulatory")
    try:
        rapport = build_report(spark, report_date, countries).cache()
        lignes = rapport.count()
        if lignes == 0:
            logger.warning("aucune donnée pour le %s", report_date)
            return 0

        layers.merge(
            spark, rapport, TABLE, layers.GOLD_NAMESPACE,
            ["reporting_date", "country_code"], "country_code",
        )

        non_conformes = rapport.filter(
            (~F.col("npl_conforme")) | (~F.col("loss_ratio_conforme"))
        ).count()
        declarations = rapport.agg(F.sum("nb_declarations_aml")).collect()[0][0] or 0
        logger.info(
            "rapport du %s : %d pays, %d en dépassement de seuil, %d déclaration(s) AML",
            report_date, lignes, non_conformes, declarations,
        )
        rapport.unpersist()
        return lignes
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.batch.regulatory",
        description="Produit les agrégats réglementaires BCEAO et CIMA d'une journée.",
    )
    parser.add_argument(
        "--report-date", required=True, metavar="AAAA-MM-JJ",
        help="journée déclarée, normalement la veille de l'exécution",
    )
    parser.add_argument("--countries", nargs="*", metavar="CC",
                        help="restreindre à certains pays")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    report_date = datetime.strptime(args.report_date, "%Y-%m-%d").date()

    lignes = run(report_date, args.countries)
    print("\nRapport réglementaire du {} — {} pays\n".format(report_date, lignes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
