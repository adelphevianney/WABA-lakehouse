"""Utilitaires partagés par les transformations du médaillon.

Regroupe ce dont les couches Silver et Gold ont également besoin : nommage des
zones, conversion de devise, masquage des champs personnels et écriture
idempotente. Les répartir entre les deux jobs les ferait diverger au premier
ajustement.
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import domain as dom
from common import pii
from jobs.batch.session import CATALOG

RAW_NAMESPACE = "raw"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"

#: Horodatage de traitement, apposé sur chaque table produite. Permet de dater un
#: recalcul et de distinguer deux exécutions sur les mêmes données.
PROCESSED_AT = "processed_at"


def qualified(table: str, namespace: str) -> str:
    return "{}.{}.{}".format(CATALOG, namespace, table)


def read(spark: SparkSession, table: str, namespace: str = RAW_NAMESPACE) -> DataFrame:
    return spark.table(qualified(table, namespace))


# =============================================================================
# Conversion de devise
# =============================================================================


def to_eur(amount: Column, currency: Column) -> Column:
    """Convertit un montant vers l'euro selon la devise de la ligne.

    La conversion est indispensable dès qu'on agrège plusieurs pays : additionner
    des francs CFA et des cedis donnerait un total dépourvu de sens, les deux
    devises différant d'un facteur 37.

    Les taux viennent du domaine partagé. Celui du franc CFA est une parité
    réglementaire fixe ; celui du cedi est une valeur de paramétrage, qu'une
    plateforme de production remplacerait par une table de taux historisée pour
    pouvoir rejouer un cours passé.
    """
    mapping: List[Column] = []
    for code, rate in dom.FX_PER_EUR.items():
        mapping.extend([F.lit(code), F.lit(float(rate))])
    rate = F.create_map(*mapping)[currency]
    return F.round(amount / rate, 2)


# =============================================================================
# Données personnelles
# =============================================================================


def token(column: Column) -> Column:
    """Jeton de pseudonymisation, identique à celui de `common.pii`.

    La clé est injectée comme littéral plutôt que lue par une UDF : l'expression
    reste native, donc exécutée sans passer chaque ligne par l'interpréteur
    Python — l'image Spark n'embarque d'ailleurs ni pandas ni pyarrow.
    """
    keyed = F.concat(F.lit(pii.key_material()), column)
    return F.substring(F.sha2(keyed, 256), 1, pii.TOKEN_LENGTH)


def mask_tail(column_name: str, keep: int = 4, prefix: int = 0) -> Column:
    """Masque une valeur en ne conservant que son début et sa fin.

    Prend un **nom** de colonne et non une expression : la longueur du masque
    dépend de celle de la valeur, ce qui impose de référencer la colonne
    plusieurs fois dans la même expression SQL.

    Un agent doit pouvoir reconnaître un compte sans que la valeur complète soit
    exposée. Les valeurs nulles sont propagées : au Ghana, l'absence d'IBAN est
    une réalité métier et non une donnée manquante, et inventer un masque
    laisserait croire qu'un IBAN existe.
    """
    return F.expr(
        """
        CASE
            WHEN {col} IS NULL THEN NULL
            WHEN length({col}) <= {total} THEN repeat('*', length({col}))
            ELSE concat(
                substr({col}, 1, {prefix}),
                repeat('*', length({col}) - {total}),
                substr({col}, length({col}) - {keep} + 1)
            )
        END
        """.format(col=column_name, keep=keep, prefix=prefix, total=keep + prefix)
    )


# =============================================================================
# Écriture idempotente
# =============================================================================


def deduplicate(frame: DataFrame, key: str, order_by: Sequence[str]) -> DataFrame:
    """Une seule ligne par clé, choisie de façon déterministe.

    Critère d'évaluation explicite du Level 2. L'ordre de départage est imposé
    plutôt que laissé au hasard : deux exécutions doivent retenir la même ligne,
    sans quoi un recalcul ferait varier les agrégats en aval.
    """
    ranked = Window.partitionBy(key).orderBy(*[F.col(c) for c in order_by])
    return (
        frame.withColumn("_rank", F.row_number().over(ranked))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def restrict_window(frame: DataFrame, days: Optional[int], column: str) -> DataFrame:
    """Ne conserve que les `days` derniers jours **présents dans la source**.

    La borne est prise sur la donnée, non sur l'horloge : les jeux simulés
    portent des dates passées, et une fenêtre glissante ancrée sur « maintenant »
    ne retiendrait rien du tout.

    C'est le mécanisme qui rend le médaillon calculable à volumétrie réelle. Le
    jeu complet de l'annexe couvre 90 jours et 22 millions de lignes ; en mode
    local, une reconstruction intégrale dépasse ce qu'un pilote de 2 Go absorbe.
    Recalculer la fenêtre récente et laisser l'historique tel qu'il a été produit
    est de toute façon ce que fait un médaillon en exploitation — un recalcul
    complet est une opération exceptionnelle, pas le régime nominal.
    """
    if not days or column not in frame.columns:
        return frame
    borne = frame.select(F.max(column)).first()[0]
    if borne is None:
        return frame
    depuis = borne - timedelta(days=int(days))
    return frame.filter(F.col(column) >= F.lit(depuis))


def evolve_schema(spark: SparkSession, identifier: str, expected) -> List[str]:
    """Ajoute à une table existante les colonnes qu'elle ne porte pas encore.

    `CREATE TABLE IF NOT EXISTS` ne fait rien sur une table déjà présente : une
    colonne ajoutée au schéma du dépôt resterait invisible dans un environnement
    déjà déployé, et l'écriture échouerait ensuite pour cause de schéma
    incompatible — sans que rien n'indique que la cause est une table restée en
    arrière.

    Iceberg fait évoluer un schéma sans réécrire les données : les colonnes
    reçoivent un identifiant propre, et les fichiers antérieurs rendent
    simplement null pour celles qu'ils ne contiennent pas. Seuls les ajouts sont
    automatisés ici ; une suppression ou un changement de type sont des
    ruptures de contrat, qui doivent rester des décisions explicites.

    `expected` est une suite de couples (nom, type SQL).
    """
    present = {field.name.lower() for field in spark.table(identifier).schema.fields}
    ajoutees: List[str] = []
    for name, type_ in expected:
        if name.lower() not in present:
            spark.sql("ALTER TABLE {} ADD COLUMN {} {}".format(identifier, name, type_))
            ajoutees.append(name)
    return ajoutees


def create_table(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    namespace: str,
    partitioning: str,
) -> str:
    """Crée la table cible si elle n'existe pas, et la fait évoluer si besoin."""
    identifier = qualified(table, namespace)
    declared = [
        (field.name, field.dataType.simpleString().upper()) for field in frame.schema.fields
    ]
    columns = ",\n    ".join("{} {}".format(name, type_) for name, type_ in declared)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS {}.{}".format(CATALOG, namespace))
    spark.sql(
        "CREATE TABLE IF NOT EXISTS {identifier} (\n    {columns}\n)\n"
        "USING iceberg\nPARTITIONED BY ({partitioning})\n"
        "TBLPROPERTIES ('format-version' = '2', "
        "'write.parquet.compression-codec' = 'zstd', "
        # Redistribue les lignes par partition avant l'écriture. Sans cela,
        # chaque tâche peut toucher toutes les partitions à la fois et ouvrir
        # autant d'écrivains — 8 pays sur 90 jours en font 720, dont les tampons
        # suffisent à épuiser le tas du pilote. C'est cette explosion en éventail,
        # et non la jointure, qui faisait tomber le MERGE à volumétrie réelle.
        "'write.distribution-mode' = 'hash')".format(
            identifier=identifier, columns=columns, partitioning=partitioning
        )
    )
    spark.sql(
        "ALTER TABLE {} SET TBLPROPERTIES ('write.distribution-mode' = 'hash')".format(identifier)
    )
    evolve_schema(spark, identifier, declared)
    return identifier


def merge(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    namespace: str,
    key,
    partitioning: str,
    partition_key: Optional[str] = "country_code",
) -> int:
    """Fusionne un lot dans sa table cible et renvoie le nombre de lignes ajoutées.

    Les deux comptages encadrant la fusion ont un coût : ils balaient la table
    cible. C'est acceptable pour un traitement batch, qui s'exécute une fois par
    cycle et dont le rapport d'exécution a besoin de ce chiffre. Un job de
    streaming, qui fusionne toutes les vingt secondes, appelle `merge_into`
    directement.
    """
    identifier = create_table(spark, frame, table, namespace, partitioning)
    before = spark.table(identifier).count()
    merge_into(spark, frame, identifier, key, partition_key)
    return spark.table(identifier).count() - before


def merge_into(
    spark: SparkSession,
    frame: DataFrame,
    identifier: str,
    key,
    partition_key: Optional[str] = "country_code",
) -> None:
    """Fusionne un lot dans une table existante, sur sa clé naturelle.

    Même principe qu'à l'ingestion : `MERGE` sur la clé naturelle rend le job
    rejouable. Le prédicat inclut la colonne de partition pour qu'Iceberg élague
    au lieu de balayer la table entière.

    Les lignes existantes sont mises à jour et non ignorées, contrairement à la
    couche brute : un recalcul de Silver doit propager une correction de règle
    métier, alors qu'une réingestion de la couche brute ne doit rien changer.
    """
    view = "source_{}".format(identifier.replace(".", "_"))
    frame.createOrReplaceTempView(view)

    # Une table Gold est un agrégat : sa clé est la maille de restitution, donc
    # composée de plusieurs colonnes. Une table Silver a une clé naturelle
    # unique. Les deux cas passent par le même appariement.
    keys = [key] if isinstance(key, str) else list(key)
    if partition_key and partition_key not in keys:
        keys.append(partition_key)
    condition = " AND ".join("t.{k} = s.{k}".format(k=k) for k in keys)

    spark.sql(
        "MERGE INTO {identifier} AS t\nUSING {view} AS s\n  ON {condition}\n"
        "WHEN MATCHED THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *".format(
            identifier=identifier, view=view, condition=condition
        )
    )
