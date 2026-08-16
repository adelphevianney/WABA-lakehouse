"""Routage du flux NiFi : le chemin d'un objet détermine son topic.

Ces tests n'ont pas besoin de NiFi. Ils vérifient l'expression que le script
transmet au serveur, en l'évaluant avec les mêmes règles que le langage
d'expression de NiFi pour les quelques fonctions de chaîne employées. Ce qu'ils
garantissent : la table de correspondance de `common.domain` est bien appliquée,
la convention de nommage du bucket est bien celle que le générateur produit, et
les référentiels ne sont rattachés à aucun topic. Ce qu'ils ne garantissent pas :
que NiFi implémente ces fonctions comme supposé — seule l'exécution du flux le
démontre.
"""

from __future__ import annotations

import re

import pytest

from common.domain import DATASETS, RAW_TOPICS, REFERENTIALS
from generator import storage
from scripts import nifi_flow

_APPEL = re.compile(r":(\w+)\(([^)]*)\)")


def evalue(expression: str, filename: str) -> str:
    """Évalue une expression NiFi restreinte aux fonctions de chaîne utilisées.

    NiFi renvoie le sujet inchangé lorsqu'un délimiteur est absent ; c'est ce
    comportement qui range les référentiels hors des topics, il est donc
    reproduit fidèlement.
    """
    prefixe = "${filename"
    assert expression.startswith(prefixe) and expression.endswith("}")

    valeur = filename
    for fonction, arguments in _APPEL.findall(expression[len(prefixe):-1]):
        args = [a.strip().strip("'") for a in arguments.split(",")] if arguments else []
        if fonction == "substringBeforeLast":
            valeur = valeur.rsplit(args[0], 1)[0] if args[0] in valeur else valeur
        elif fonction == "substringAfterLast":
            valeur = valeur.rsplit(args[0], 1)[-1] if args[0] in valeur else valeur
        elif fonction == "substringBefore":
            valeur = valeur.split(args[0], 1)[0] if args[0] in valeur else valeur
        elif fonction == "replace":
            valeur = valeur.replace(args[0], args[1])
        else:
            raise AssertionError("fonction non prise en charge : {}".format(fonction))
    return valeur


def cle_de_transaction(kind: str, country_code: str = "CI") -> str:
    """Reproduit la nomenclature de dépôt, sans passer par MinIO."""
    return "{0}/{1}/{1}_{0}_20260331_01.csv".format(country_code, kind)


@pytest.mark.parametrize("kind, topic", sorted(RAW_TOPICS.items()))
def test_chaque_jeu_de_donnees_part_vers_son_topic(kind, topic):
    assert evalue(nifi_flow.topic_expression(), cle_de_transaction(kind)) == topic


@pytest.mark.parametrize("kind", sorted(RAW_TOPICS))
def test_le_jeu_de_donnees_est_deduit_du_chemin(kind):
    assert evalue(nifi_flow.dataset_expression(), cle_de_transaction(kind)) == kind


@pytest.mark.parametrize("name", sorted(REFERENTIALS))
def test_les_referentiels_ne_sont_rattaches_a_aucun_topic(name):
    """Ce sont des données de référence, pas des événements : le batch les
    charge, le streaming les ignore."""
    cle = "{}/{}.csv".format(storage.REFERENTIALS_PREFIX, name)
    assert not evalue(nifi_flow.topic_expression(), cle).startswith(nifi_flow.TOPIC_PREFIX)


def test_chaque_jeu_de_donnees_du_batch_a_son_topic():
    """Un jeu de données ingéré par le batch mais absent de la table des topics
    ne produirait aucune erreur : ses fichiers seraient simplement écartés comme
    des référentiels."""
    assert set(RAW_TOPICS) == set(DATASETS)


def test_un_topic_hors_convention_est_refuse(monkeypatch):
    """Le routage repose sur le préfixe `raw-` : le renommer sans le savoir
    produirait un flux qui n'échoue jamais et ne publie rien."""
    monkeypatch.setattr(nifi_flow, "RAW_TOPICS", {"bank_txn": "transactions-brutes"})
    with pytest.raises(RuntimeError, match="préfixe"):
        nifi_flow.topic_expression()
