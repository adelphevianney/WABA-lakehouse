"""Règles de validation de la couche brute.

Le §1.3 demande « une validation basique (rejet des lignes malformées) et un
contrôle des devises ». Rejeter suppose de savoir pourquoi : chaque ligne
écartée sort avec un motif lisible, conservé dans une table dédiée. Une
validation qui se contenterait de filtrer produirait un écart de volumétrie
inexplicable entre la source et la cible.

Les contrôles sont évalués **avant** le typage, sur les colonnes encore
textuelles. C'est ce qui permet de distinguer une valeur absente d'une valeur
présente mais non convertible : après un `CAST`, les deux sont nulles et
l'information est perdue.
"""

from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import Column as SparkColumn
from pyspark.sql import functions as F

from common import domain as dom
from jobs.batch.schemas import CORRUPT_RECORD, DatasetSpec

#: Types dont la conversion depuis le texte peut échouer silencieusement.
_CASTABLE = {"DOUBLE", "INT", "BIGINT", "TIMESTAMP", "DATE", "BOOLEAN"}


def _currency_map() -> SparkColumn:
    pairs: List[SparkColumn] = []
    for country, currency in dom.CURRENCY_BY_COUNTRY.items():
        pairs.extend([F.lit(country), F.lit(currency)])
    return F.create_map(*pairs)


def _entity_matrix() -> SparkColumn:
    pairs: List[SparkColumn] = []
    for entity, countries in dom.ENTITY_COUNTRIES.items():
        pairs.extend([F.lit(entity), F.array(*[F.lit(c) for c in countries])])
    return F.create_map(*pairs)


def cast_expression(name: str, type_: str) -> SparkColumn:
    """Conversion depuis le texte vers le type cible.

    Les dates transitent par un horodatage : le générateur sérialise toutes les
    colonnes temporelles au format ISO 8601 complet, y compris celles qui ne
    portent qu'une date, et une conversion directe en DATE dépendrait de la
    tolérance du moteur au suffixe horaire.
    """
    if type_ == "DATE":
        return F.col(name).cast("timestamp").cast("date")
    return F.col(name).cast(type_.lower())


def rejection_reason(spec: DatasetSpec) -> SparkColumn:
    """Motif de rejet d'une ligne, ou null si elle est valide.

    Les contrôles sont ordonnés du plus structurel au plus métier : la première
    règle qui échoue donne le motif, ce qui évite d'empiler des messages
    redondants sur une ligne cassée.
    """
    checks: List[Tuple[SparkColumn, str]] = []

    # 1. Ligne structurellement illisible (colonnes manquantes, guillemets
    #    déséquilibrés). Spark l'a rangée dans la colonne dédiée.
    checks.append((F.col(CORRUPT_RECORD).isNotNull(), "ligne illisible"))

    # 2. Clés et colonnes obligatoires.
    for column in spec.columns:
        if column.required:
            checks.append((F.col(column.name).isNull(), f"{column.name} manquant"))

    # 3. Valeurs présentes mais non convertibles vers le type déclaré.
    for column in spec.columns:
        if column.type in _CASTABLE:
            not_castable = F.col(column.name).isNotNull() & cast_expression(
                column.name, column.type
            ).isNull()
            checks.append((not_castable, f"{column.name} non convertible en {column.type}"))

    # 4. Code pays hors du périmètre du groupe.
    country = spec.country_column
    checks.append((
        F.col(country).isNotNull() & ~F.col(country).isin(list(dom.COUNTRY_CODES)),
        f"{country} hors périmètre",
    ))

    # 5. Valeurs hors nomenclature.
    for name, allowed in spec.enums.items():
        checks.append((
            F.col(name).isNotNull() & ~F.col(name).isin(list(allowed)),
            f"{name} hors nomenclature",
        ))

    # 6. Devise incohérente avec le pays — contrôle explicitement demandé.
    if spec.currency_column:
        expected = _currency_map()[F.col(country)]
        checks.append((
            F.col(spec.currency_column).isNotNull()
            & expected.isNotNull()
            & (F.col(spec.currency_column) != expected),
            "devise incohérente avec le pays",
        ))

    # 7. Entité déclarée dans un pays où elle n'opère pas.
    if spec.check_entity_matrix and "entity_type" in {c.name for c in spec.columns}:
        allowed_countries = _entity_matrix()[F.col("entity_type")]
        checks.append((
            F.col("entity_type").isNotNull()
            & allowed_countries.isNotNull()
            & ~F.array_contains(allowed_countries, F.col(country)),
            "entité absente de ce pays",
        ))

    # 8. Montants négatifs là où ils n'ont pas de sens.
    for name in spec.non_negative:
        checks.append((
            cast_expression(name, "DOUBLE") < F.lit(0),
            f"{name} négatif",
        ))

    expression = F.lit(None).cast("string")
    for condition, reason in reversed(checks):
        expression = F.when(condition, F.lit(reason)).otherwise(expression)
    return expression
