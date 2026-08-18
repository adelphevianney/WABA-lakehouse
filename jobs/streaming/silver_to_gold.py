"""Job 2 — Silver vers Gold en streaming : fraude, AML et liquidité (§3.3).

Consomme les topics `silver-*` produits par le Job 1 et alimente trois topics
d'alertes, doublés de trois tables Iceberg `gold.*`.

    silver-bank-transactions ──┬─▶ règle 1 (rafales, fenêtre glissante) ─▶ gold-fraud-alerts
                               ├─▶ AML (seuil déclaratif)               ─▶ gold-aml-events
                               └─▶ couverture de liquidité (fenêtre)    ─▶ gold-liquidity-alerts
    silver-mobile-money       ───▶ règle 2 (origine inhabituelle)       ─▶ gold-fraud-alerts
    silver-insurance-ops      ───▶ règle 3 (sinistre excessif)          ─▶ gold-fraud-alerts

Trois queries, et non cinq : les règles qui s'évaluent ligne à ligne — AML,
origine inhabituelle, sinistre excessif — partagent une même query qui lit les
trois topics et les aiguille dans son micro-lot. Seules les deux règles à
fenêtre, qui exigent une agrégation dans le plan de streaming, en ont une à
elles. C'est autant d'état et de threads en moins sur une machine où la mémoire
est la ressource contrainte.

Le schéma de lecture n'est pas redéclaré : il est lu sur la table Iceberg
correspondante. Le Job 1 écrit le même DataFrame dans le topic et dans la table,
si bien que le schéma de la table *est* le contrat du topic.

Les règles sont celles de `generator/anomalies.py`, qui en donne une
implémentation de référence en pandas. Cet oracle sert à vérifier que le job
retrouve bien les anomalies injectées — c'est ce qui rend la validation objective
plutôt que déclarative.

Exemples :
    python -m jobs.streaming.silver_to_gold --once
    python -m jobs.streaming.silver_to_gold --rules rafales aml
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common import domain as dom
from jobs.batch import layers
from jobs.batch.session import build_session
from jobs.streaming import iceberg_sink, kafka_io

logger = logging.getLogger("jobs.streaming.silver_to_gold")

# =============================================================================
# Paramètres
# =============================================================================

#: Table Silver dont le schéma sert de contrat au topic correspondant.
SILVER_TABLE_OF: Dict[str, str] = {
    "bank_txn": "bank_transactions",
    "insurance_ops": "insurance_operations",
    "mobile_money": "mobile_money_payments",
}

#: Clé naturelle de chaque flux. Son absence après analyse signale un message
#: que le job ne sait pas exploiter, et qui part en file de rebut.
KEY_OF: Dict[str, str] = {
    "bank_txn": "transaction_id",
    "insurance_ops": "operation_id",
    "mobile_money": "payment_id",
}

FRAUD_TABLE = "fraud_alerts"
AML_TABLE = "aml_events"
LIQUIDITY_TABLE = "liquidity_alerts"

#: Fenêtre glissante des règles à état, telle que fixée par l'énoncé.
WINDOW_SIZE = "5 minutes"
WINDOW_SLIDE = "1 minute"

#: Tolérance au désordre sur l'horodatage métier.
#:
#: Ces règles portent sur le temps de l'événement, non sur celui de son arrivée :
#: une rafale se définit par cinq minutes vécues par le compte, pas par cinq
#: minutes de traitement. Le filigrane doit donc suivre l'horodatage métier, et
#: être assez large pour couvrir l'étalement d'un fichier rejoué — une journée
#: entière d'opérations peut arriver dans un même micro-lot. En production, où un
#: événement Silver parvient au job quelques secondes après la transaction,
#: quelques minutes suffiraient.
WATERMARK = os.getenv("WABA_STREAM_WATERMARK", "12 hours")

#: Part des encours d'un pays au-delà de laquelle les sorties nettes d'une
#: fenêtre déclenchent une alerte de liquidité.
#:
#: La valeur réglementaire vit dans `common.domain`. Elle reste hors d'atteinte
#: sur un jeu de données échantillonné : quelques centaines de transactions par
#: pays et par jour ne pèsent rien face aux encours de 250 000 comptes, et le
#: ratio observé est inférieur au millionième. Le seuil est donc surchargeable,
#: pour que le mécanisme se démontre sans travestir la norme qu'il implémente.
LIQUIDITY_RATIO = float(os.getenv("WABA_LIQUIDITY_RATIO", dom.LIQUIDITY_OUTFLOW_RATIO))


def _map_of(mapping: Dict[str, float]) -> Column:
    pairs: List[Column] = []
    for key, value in mapping.items():
        pairs.extend([F.lit(key), F.lit(float(value))])
    return F.create_map(*pairs)


def alert_id(*parts) -> Column:
    """Identifiant stable d'une alerte, support de son idempotence.

    Une même fenêtre peut être réémise lorsqu'un événement supplémentaire la
    complète : dérivé de son contenu, l'identifiant permet au `MERGE` de mettre
    l'alerte à jour au lieu de l'ajouter une seconde fois.
    """
    return F.substring(F.sha2(F.concat_ws("|", *parts), 256), 1, 32)


# =============================================================================
# Lecture des topics Silver
# =============================================================================


def silver_schema(spark: SparkSession, dataset: str) -> StructType:
    """Schéma du topic Silver, lu sur la table que le Job 1 alimente en parallèle."""
    return spark.table(
        layers.qualified(SILVER_TABLE_OF[dataset], layers.SILVER_NAMESPACE)
    ).schema


#: Fenêtre de déduplication de l'entrée.
#:
#: Le Job 1 écrit dans Iceberg puis dans Kafka, sans transaction commune aux
#: deux : un incident entre les deux écritures fait rejouer le micro-lot et le
#: topic reçoit un doublon. C'est ici qu'il est absorbé. Sans cette étape, une
#: transaction reçue trois fois compte trois fois — et une seule opération de
#: montant élevé suffit alors à constituer une fausse rafale.
INPUT_DEDUP_WINDOW = os.getenv("WABA_INPUT_DEDUP_WINDOW", "10 minutes")


def read_silver(spark: SparkSession, datasets: List[str], starting_offsets: str) -> DataFrame:
    """Flux brut d'un ou plusieurs topics Silver, message non analysé compris."""
    topics = [dom.SILVER_TOPICS[dataset] for dataset in datasets]
    return kafka_io.read_topics(spark, topics, starting_offsets)


def dedup_on_arrival(frame: DataFrame, datasets: List[str]) -> DataFrame:
    """Déduplique sur la clé naturelle, extraite sans analyser tout le message.

    Employée par la query des règles unitaires, qui lit trois topics de schémas
    différents et n'a besoin ici que de l'identifiant. Une empreinte du message
    ne conviendrait pas : le Job 1 y appose son horodatage de traitement, si bien
    que deux publications du même événement diffèrent d'un octet.

    Le filigrane porte sur l'heure d'arrivée, car ces règles sont sans état :
    seule compte la proximité des deux copies, pas la date de l'événement.
    """
    cle = F.coalesce(*[
        F.get_json_object(F.col("payload"), "$." + KEY_OF[dataset]) for dataset in datasets
    ])
    return (
        frame
        .withColumn("_cle", F.coalesce(cle, F.sha2(F.coalesce(F.col("payload"), F.lit("")), 256)))
        .withWatermark("kafka_timestamp", INPUT_DEDUP_WINDOW)
        .dropDuplicatesWithinWatermark(["_cle"])
    )


def bank_events(spark: SparkSession, source: DataFrame) -> DataFrame:
    """Transactions bancaires analysées, dédupliquées, prêtes pour une fenêtre.

    Le filigrane est posé une fois pour toutes sur l'horodatage métier, et sert
    à la fois à la déduplication et aux agrégations qui suivent : Spark refuse
    qu'un même flux en redéfinisse un second.
    """
    return (
        parse_silver(spark, source, "bank_txn")
        .filter(F.col("transaction_id").isNotNull())
        .withWatermark("timestamp", WATERMARK)
        .dropDuplicatesWithinWatermark(["transaction_id"])
    )


def parse_silver(spark: SparkSession, frame: DataFrame, dataset: str) -> DataFrame:
    """Analyse les messages d'un topic Silver selon le schéma de sa table."""
    parsed = frame.withColumn(
        "_e", F.from_json(F.col("payload"), silver_schema(spark, dataset))
    )
    return parsed.select("_e.*", "kafka_timestamp", "payload", "topic", "partition", "offset")


def polices_de(spark: SparkSession) -> DataFrame:
    """Primes annuelles des polices, référence de la règle 3.

    Reconstruite depuis la session du micro-lot plutôt que transmise : une
    session clonée ne peut pas joindre un DataFrame attaché à une autre. Le plan
    étant identique, le gestionnaire de cache — partagé entre les sessions — le
    reconnaît et ne relit pas la table.
    """
    return (
        spark.table(layers.qualified("accounts", layers.SILVER_NAMESPACE))
        .filter(F.col("annual_premium_eur").isNotNull())
        .select(F.col("account_id"), F.col("annual_premium_eur"))
    )


def encours_de(spark: SparkSession) -> DataFrame:
    """Encours par pays, dénominateur du ratio de couverture de liquidité.

    Les comptes clos ou dormants sont exclus : les compter gonflerait le
    dénominateur d'encours qui ne sont plus mobilisables.
    """
    return (
        spark.table(layers.qualified("accounts", layers.SILVER_NAMESPACE))
        .filter(F.col("est_ouvert"))
        .groupBy("country_code")
        .agg(F.sum("balance_eur").alias("encours_eur"))
    )


# =============================================================================
# Schéma commun des alertes de fraude
# =============================================================================


def fraud_alert(
    frame: DataFrame,
    alert_type: str,
    rule: str,
    subject: str,
    subject_kind: str,
    event_time: Column,
    amount_eur: Column,
    reference_eur: Column,
    occurrences: Column,
    detail: Column,
    window_start: Optional[Column] = None,
    window_end: Optional[Column] = None,
) -> DataFrame:
    """Projette une détection dans le format commun des alertes de fraude.

    Les trois règles produisent des objets de nature différente — une rafale
    porte sur une fenêtre et un compte, un sinistre sur une opération. Les
    ramener à un schéma unique est ce qui permet de les publier dans un topic
    commun et de les interroger d'une seule requête : un analyste fraude
    travaille sur un flux d'alertes, pas sur trois.
    """
    return frame.select(
        # L'identité de l'alerte repose sur l'instant de l'événement, jamais sur
        # les bornes de la fenêtre : celles-ci varient d'une exécution à l'autre
        # selon le découpage des micro-lots, là où le premier virement d'une
        # rafale ou l'horodatage d'un sinistre ne bougent pas.
        alert_id(F.lit(alert_type), F.col(subject), event_time.cast("string"))
        .alias("alert_id"),
        F.lit(alert_type).alias("alert_type"),
        F.lit(rule).alias("rule"),
        F.col("country_code"),
        F.col("entity_type"),
        F.lit(subject_kind).alias("subject_kind"),
        F.col(subject).alias("subject_id"),
        event_time.alias("event_time"),
        (window_start if window_start is not None else F.lit(None).cast("timestamp"))
        .alias("window_start"),
        (window_end if window_end is not None else F.lit(None).cast("timestamp"))
        .alias("window_end"),
        occurrences.cast("int").alias("occurrences"),
        F.round(amount_eur, 2).alias("amount_eur"),
        F.round(reference_eur, 2).alias("reference_eur"),
        detail.alias("detail"),
        F.current_timestamp().alias("detected_at"),
    )


def publish_fraud(spark: SparkSession, alerts: DataFrame) -> int:
    """Double écriture d'un lot d'alertes : table Gold puis topic.

    Le dédoublonnage sur l'identifiant n'est pas une précaution de style : un
    `MERGE` dont la source présente deux fois la même clé échoue, et sur une
    table encore vide il insérerait deux lignes identiques.
    """
    alerts = alerts.dropDuplicates(["alert_id"]).persist()
    try:
        count = alerts.count()
        if count:
            iceberg_sink.merge_micro_batch(
                spark, alerts, FRAUD_TABLE, layers.GOLD_NAMESPACE,
                "alert_id", "country_code, days(event_time)",
            )
            kafka_io.publish(alerts, dom.GOLD_FRAUD_TOPIC, key="alert_id")
        return count
    finally:
        alerts.unpersist()


# =============================================================================
# Règle 1 — rafales de virements de montant élevé (fenêtre glissante)
# =============================================================================


def bursts_stream(spark: SparkSession, source: DataFrame) -> DataFrame:
    """Comptage des virements de montant élevé par compte et par fenêtre.

    Le seuil est évalué en devise locale : 500 000 XOF et 2 500 GHS ne sont pas
    le même montant, et convertir avant de comparer reviendrait à appliquer au
    Ghana un seuil calibré pour la zone franc.
    """
    seuil = _map_of(dom.FRAUD_BURST_AMOUNT)[F.col("currency")]

    return (
        bank_events(spark, source)
        # Le statut n'entre pas dans la règle : une rafale de virements refusés
        # est un signal au moins aussi fort qu'une rafale aboutie.
        .filter(F.col("amount") > seuil)
        .groupBy(
            F.window(F.col("timestamp"), WINDOW_SIZE, WINDOW_SLIDE).alias("w"),
            F.col("account_id"), F.col("country_code"),
            F.col("entity_type"), F.col("currency"),
        )
        .agg(
            F.count("*").alias("occurrences"),
            F.sum("amount_eur").alias("montant_eur"),
            F.min("timestamp").alias("premier"),
            F.max("timestamp").alias("dernier"),
        )
        .filter(F.col("occurrences") >= F.lit(dom.FRAUD_BURST_MIN_COUNT))
    )


def collapse_bursts(frame: DataFrame) -> DataFrame:
    """Ramène à une alerte les fenêtres qui décrivent la même rafale.

    Une fenêtre de cinq minutes qui avance d'une minute produit cinq fenêtres
    recouvrantes : une rafale entièrement contenue dans l'une l'est aussi dans
    les suivantes, et le flux émet la même rafale jusqu'à cinq fois. Un analyste
    fraude reçoit alors cinq alertes pour un seul épisode.

    Toutes ces fenêtres partagent la même première transaction. Regrouper sur le
    couple compte / première transaction les rassemble exactement, sans heuristique
    de recouvrement : une fenêtre qui engloberait un virement antérieur décrirait
    un épisode réellement plus large, et mérite alors sa propre alerte.

    Le regroupement est aussi une nécessité technique : un `MERGE` dont la source
    contient deux fois la même clé échoue.
    """
    return frame.groupBy(
        "account_id", "country_code", "entity_type", "currency", "premier"
    ).agg(
        F.max("occurrences").alias("occurrences"),
        F.max("montant_eur").alias("montant_eur"),
        F.max("dernier").alias("dernier"),
        F.min("w.start").alias("window_start"),
        F.max("w.end").alias("window_end"),
    )


def bursts_alerts(frame: DataFrame) -> DataFrame:
    frame = collapse_bursts(frame)
    seuil = _map_of(dom.FRAUD_BURST_AMOUNT)[F.col("currency")]
    detail = F.concat(
        F.col("occurrences").cast("string"),
        F.lit(" virements > "), F.format_number(seuil, 0), F.lit(" "), F.col("currency"),
        F.lit(" depuis le même compte en "),
        F.round((F.col("dernier").cast("double") - F.col("premier").cast("double")) / 60, 1)
         .cast("string"),
        F.lit(" min"),
    )
    return fraud_alert(
        frame, "RAFALE_VIREMENTS", "fraude_rafale", "account_id", "compte",
        event_time=F.col("premier"),
        amount_eur=F.col("montant_eur"),
        reference_eur=F.lit(None).cast("double"),
        occurrences=F.col("occurrences"),
        detail=detail,
        window_start=F.col("window_start"), window_end=F.col("window_end"),
    )


# =============================================================================
# Règle 2 — paiement mobile money depuis un pays inhabituel
# =============================================================================


def foreign_origin_alerts(parsed: DataFrame) -> DataFrame:
    """Le Job 1 a déjà rapproché l'émetteur de son pays de rattachement.

    La colonne `origine_inhabituelle` de Silver porte cette comparaison : la
    règle se réduit ici à un filtre, ce qui est le signe que l'enrichissement a
    été fait à la bonne couche.
    """
    parsed = parsed.filter(F.col("origine_inhabituelle"))
    detail = F.concat(
        F.lit("paiement émis depuis "), F.col("sender_country"),
        F.lit(" par un client rattaché à un autre pays — corridor "), F.col("corridor"),
    )
    return fraud_alert(
        parsed, "ORIGINE_INHABITUELLE", "fraude_pays_inhabituel", "sender_id", "client",
        event_time=F.col("timestamp"),
        amount_eur=F.col("amount_eur"),
        reference_eur=F.lit(None).cast("double"),
        occurrences=F.lit(1),
        detail=detail,
    )


# =============================================================================
# Règle 3 — sinistre disproportionné par rapport à la prime
# =============================================================================


def excessive_claim_alerts(parsed: DataFrame, polices: DataFrame) -> DataFrame:
    """Compare le règlement à la prime annuelle du contrat.

    La prime vient du référentiel des comptes, non des échéances observées : sur
    trois jours de données, une police sur cinq seulement a cotisé, et se rabattre
    sur une prime médiane de branche produirait des faux positifs en série — les
    primes s'étalant sur un facteur dix, un sinistre normal sur une police chère
    dépasserait trois fois la médiane sans rien avoir d'anormal.
    """
    parsed = parsed.filter(F.col("operation_type") == F.lit("CLAIM_PAYMENT"))
    joint = parsed.join(polices, on="account_id", how="inner").filter(
        (F.col("annual_premium_eur") > 0)
        & (F.col("amount_eur") > F.lit(dom.FRAUD_CLAIM_PREMIUM_RATIO) * F.col("annual_premium_eur"))
    )
    detail = F.concat(
        F.lit("sinistre "), F.col("product_line"), F.lit(" réglé à "),
        F.round(F.col("amount_eur") / F.col("annual_premium_eur"), 1).cast("string"),
        F.lit(" fois la prime annuelle du contrat"),
    )
    return fraud_alert(
        joint, "SINISTRE_EXCESSIF", "fraude_sinistre_excessif", "account_id", "police",
        event_time=F.col("timestamp"),
        amount_eur=F.col("amount_eur"),
        reference_eur=F.col("annual_premium_eur"),
        occurrences=F.lit(1),
        detail=detail,
    )


# =============================================================================
# Surveillance AML
# =============================================================================


def aml_events(parsed: DataFrame) -> DataFrame:
    """Virements dépassant le seuil déclaratif de la zone.

    L'énoncé vise les virements : un retrait au distributeur ou un paiement
    marchand du même montant ne relève pas de la même obligation. La colonne
    `depasse_seuil_aml` de Silver porte déjà cette distinction.
    """
    parsed = parsed.filter(F.col("depasse_seuil_aml"))
    seuil = _map_of(dom.AML_THRESHOLD)[F.col("currency")]
    return parsed.select(
        F.col("transaction_id").alias("event_id"),
        F.col("transaction_id"),
        F.col("timestamp"),
        F.col("account_id"),
        F.col("customer_token"),
        F.col("country_code"),
        F.col("entity_type"),
        F.col("transaction_type"),
        F.col("channel"),
        F.col("currency"),
        F.col("amount"),
        F.col("amount_eur"),
        seuil.alias("threshold"),
        F.round(F.col("amount") / seuil, 2).alias("threshold_ratio"),
        F.col("beneficiary_country"),
        F.col("est_transfrontalier"),
        F.col("source_file"),
        F.current_timestamp().alias("detected_at"),
    )


# =============================================================================
# Couverture de liquidité
# =============================================================================


def liquidity_stream(spark: SparkSession, source: DataFrame) -> DataFrame:
    """Sorties nettes par pays et par fenêtre.

    Un retrait et un paiement quittent la banque ; un virement ne quitte le pays
    que s'il est transfrontalier — un virement interne déplace le solde d'un
    compte à l'autre sans réduire la liquidité du marché. Les dépôts viennent en
    déduction, d'où « nettes ».
    """
    sortie = (
        F.when(F.col("transaction_type").isin("WITHDRAWAL", "PAYMENT"), F.col("amount_eur"))
        .when(F.col("transaction_type").isin(list(dom.WIRE_TRANSACTION_TYPES))
              & F.col("est_transfrontalier"), F.col("amount_eur"))
        .when(F.col("transaction_type") == F.lit("DEPOSIT"), -F.col("amount_eur"))
        .otherwise(F.lit(0.0))
    )
    return (
        bank_events(spark, source)
        # Seules les opérations abouties déplacent de la trésorerie : compter un
        # retrait refusé comme une sortie fausserait la couverture.
        .filter(F.col("est_aboutie"))
        .withColumn("_sortie", sortie)
        .groupBy(
            F.window(F.col("timestamp"), WINDOW_SIZE, WINDOW_SLIDE).alias("w"),
            F.col("country_code"),
        )
        .agg(
            F.sum("_sortie").alias("sorties_nettes_eur"),
            F.count("*").alias("operations"),
        )
        .filter(F.col("sorties_nettes_eur") > 0)
    )


def liquidity_alerts(frame: DataFrame, encours: DataFrame) -> DataFrame:
    joint = frame.join(encours, on="country_code", how="inner").withColumn(
        "ratio", F.col("sorties_nettes_eur") / F.col("encours_eur")
    ).filter(F.col("ratio") > F.lit(LIQUIDITY_RATIO))

    return joint.select(
        alert_id(F.lit("LIQUIDITE"), F.col("country_code"),
                 F.col("w.start").cast("string")).alias("alert_id"),
        F.col("country_code"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("operations").cast("int").alias("operations"),
        F.round(F.col("sorties_nettes_eur"), 2).alias("sorties_nettes_eur"),
        F.round(F.col("encours_eur"), 2).alias("encours_eur"),
        F.col("ratio"),
        F.lit(LIQUIDITY_RATIO).alias("seuil"),
        F.current_timestamp().alias("detected_at"),
    )


# =============================================================================
# Traitement des micro-lots
# =============================================================================


def publish_dlq(rejets: DataFrame, dataset: str) -> int:
    """Un message Silver inexploitable ne doit pas disparaître en silence.

    L'analyse d'un message dont le JSON est cassé produit des champs tous nuls,
    clé naturelle comprise : c'est cette absence qui le signale, sans qu'il soit
    besoin d'un second passage d'analyse.
    """
    payload = rejets.select(
        F.lit(dataset).alias("dataset"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("received_at"),
        F.lit("message Silver inexploitable — clé naturelle absente").alias("rejection_reason"),
        F.lit(None).cast("string").alias("source_file"),
        F.lit("silver_to_gold").alias("stage"),
        F.col("payload").alias("original_message"),
    ).persist()
    try:
        count = payload.count()
        if count:
            kafka_io.publish(payload, dom.DLQ_TOPIC, key="dataset")
        return count
    finally:
        payload.unpersist()


def process_unit_rules(batch: DataFrame, batch_id: int) -> None:
    """Règles ligne à ligne : AML, origine inhabituelle, sinistre excessif."""
    if batch.isEmpty():
        return
    session = batch.sparkSession
    batch = batch.persist()
    analyses: Dict[str, DataFrame] = {}
    try:
        rebuts = 0
        for dataset, topic in dom.SILVER_TOPICS.items():
            parsed = parse_silver(
                session, batch.filter(F.col("topic") == F.lit(topic)), dataset
            ).persist()
            analyses[dataset] = parsed
            rebuts += publish_dlq(parsed.filter(F.col(KEY_OF[dataset]).isNull()), dataset)

        valides = {
            dataset: frame.filter(F.col(KEY_OF[dataset]).isNotNull())
            for dataset, frame in analyses.items()
        }

        aml = aml_events(valides["bank_txn"]).dropDuplicates(["event_id"]).persist()
        try:
            nb_aml = aml.count()
            if nb_aml:
                iceberg_sink.merge_micro_batch(
                    session, aml, AML_TABLE, layers.GOLD_NAMESPACE,
                    "event_id", "country_code, days(timestamp)",
                )
                kafka_io.publish(aml, dom.GOLD_AML_TOPIC, key="country_code")
        finally:
            aml.unpersist()

        alertes = foreign_origin_alerts(valides["mobile_money"]).unionByName(
            excessive_claim_alerts(valides["insurance_ops"], polices_de(session))
        )
        nb_fraude = publish_fraud(session, alertes)

        logger.info("règles unitaires lot %d — %d AML, %d fraude(s), %d rebut(s)",
                    batch_id, nb_aml, nb_fraude, rebuts)
    finally:
        for frame in analyses.values():
            frame.unpersist()
        batch.unpersist()


def process_bursts(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    nb = publish_fraud(batch.sparkSession, bursts_alerts(batch))
    logger.info("rafales lot %d — %d alerte(s)", batch_id, nb)


def process_liquidity(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    session = batch.sparkSession
    alertes = liquidity_alerts(batch, encours_de(session)).dropDuplicates(["alert_id"]).persist()
    try:
        nb = alertes.count()
        if nb:
            iceberg_sink.merge_micro_batch(
                session, alertes, LIQUIDITY_TABLE, layers.GOLD_NAMESPACE,
                "alert_id", "country_code, days(window_start)",
            )
            kafka_io.publish(alertes, dom.GOLD_LIQUIDITY_TOPIC, key="country_code")
        logger.info("liquidité lot %d — %d alerte(s)", batch_id, nb)
    finally:
        alertes.unpersist()


# =============================================================================
# Orchestration
# =============================================================================

RULES = ("unitaires", "rafales", "liquidite")


def _writer(frame: DataFrame, name: str, mode: str, once: bool):
    writer = (
        frame.writeStream
        .queryName("silver_to_gold_{}".format(name))
        .option("checkpointLocation", kafka_io.checkpoint("silver_to_gold/" + name))
        .outputMode(mode)
    )
    return writer.trigger(availableNow=True) if once else writer.trigger(processingTime="20 seconds")


def run(rules: Optional[List[str]] = None, once: bool = False,
        starting_offsets: str = "earliest") -> int:
    selected = rules or list(RULES)
    spark = build_session("waba-silver-to-gold")

    # Les deux références sont relues à chaque micro-lot par les jointures. Les
    # matérialiser une fois évite de balayer 250 000 comptes toutes les vingt
    # secondes ; le cache étant partagé entre la session et ses clones, les
    # micro-lots le retrouvent en reconstruisant le même plan.
    polices_de(spark).cache().count()
    encours_de(spark).cache().count()

    requetes = []
    if "unitaires" in selected:
        datasets = list(dom.SILVER_TOPICS)
        source = dedup_on_arrival(read_silver(spark, datasets, starting_offsets), datasets)
        requetes.append(
            _writer(source, "unitaires", "append", once)
            .foreachBatch(process_unit_rules)
            .start()
        )
    if "rafales" in selected:
        flux = bursts_stream(spark, read_silver(spark, ["bank_txn"], starting_offsets))
        requetes.append(
            # Mode « update » et non « append » : une alerte de fraude doit
            # partir dès que la rafale est constituée, pas à la fermeture de la
            # fenêtre. En « append », l'alerte attendrait que le filigrane
            # dépasse la fin de fenêtre — et sur une exécution bornée, les
            # dernières fenêtres ne seraient jamais émises.
            _writer(flux, "rafales", "update", once)
            .foreachBatch(process_bursts)
            .start()
        )
    if "liquidite" in selected:
        flux = liquidity_stream(spark, read_silver(spark, ["bank_txn"], starting_offsets))
        requetes.append(
            _writer(flux, "liquidite", "update", once)
            .foreachBatch(process_liquidity)
            .start()
        )

    logger.info("%d règle(s) démarrée(s) : %s", len(requetes), ", ".join(selected))
    try:
        if once:
            for requete in requetes:
                requete.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("arrêt demandé, fermeture des flux")
        for requete in requetes:
            requete.stop()

    echecs = [r for r in requetes if r.exception() is not None]
    for requete in echecs:
        logger.error("%s a échoué : %s", requete.name, requete.exception())

    spark.stop()
    return 1 if echecs else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.streaming.silver_to_gold",
        description="Détection de fraude, surveillance AML et alertes de liquidité "
                    "depuis les topics silver-*.",
    )
    parser.add_argument("--rules", nargs="*", choices=list(RULES),
                        help="règles à activer (défaut : toutes)")
    parser.add_argument("--once", action="store_true",
                        help="traiter ce qui est disponible puis s'arrêter")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"],
                        help="position de lecture au premier démarrage (défaut : earliest)")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    return run(rules=args.rules, once=args.once, starting_offsets=args.starting_offsets)


if __name__ == "__main__":
    sys.exit(main())
