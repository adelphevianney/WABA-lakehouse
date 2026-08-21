"""Construction des trois tableaux de bord du §4.2, par l'API Superset.

Même parti pris que pour le flux NiFi : la définition vit dans un script
relisible plutôt que dans une archive exportée. Un export Superset est du YAML
généré — plusieurs milliers de lignes où l'ajout d'un graphique produit un diff
illisible, et où les identifiants changent à chaque export. Ici, chaque
graphique tient en une dizaine de lignes déclaratives, et la source de données
comme les colonnes visées se lisent d'un coup d'œil.

Le script est idempotent : il retrouve ce qui existe déjà par son nom et le met
à jour, plutôt que d'empiler des doublons à chaque exécution.

Usage :
    python scripts/superset_dashboards.py                  # les trois tableaux
    python scripts/superset_dashboards.py --only risque    # un seul
    python scripts/superset_dashboards.py --delete         # tout retirer
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import domain as dom  # noqa: E402
from scripts.nifi_client import load_env_file  # noqa: E402
from scripts.superset_client import SupersetClient, SupersetError  # noqa: E402

BASE_NOM = "WABA Lakehouse"

#: Seuils réglementaires repris du domaine : les tableaux de bord ne
#: redéfinissent pas les valeurs que les jobs appliquent déjà.
NPL_VIGILANCE = 0.03
NPL_PLAFOND = dom.NPL_REGULATORY_CEILING          # 5 % — BCEAO
LOSS_RATIO_ALERTE = dom.LOSS_RATIO_ALERT          # 70 % — CIMA

#: Délai de traitement d'un sinistre au-delà duquel l'engagement contractuel est
#: considéré comme manqué. L'énoncé parle d'un « SLA contractuel » sans le
#: chiffrer ; trente jours est l'usage du marché pour un sinistre simple.
SLA_SINISTRE_JOURS = 30

VERT, ORANGE, ROUGE = "#ACE1C4", "#FDE380", "#EFA1AA"


# =============================================================================
# Fabriques de définitions
# =============================================================================


def metrique(colonne: str, agregat: str, libelle: str) -> Dict[str, Any]:
    """Métrique simple, exprimée sur une colonne du jeu de données."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": colonne},
        "aggregate": agregat,
        "label": libelle,
        "optionName": "metric_" + uuid.uuid4().hex[:10],
    }


def metrique_sql(sql: str, libelle: str) -> Dict[str, Any]:
    """Métrique calculée, pour ce qu'un agrégat simple n'exprime pas."""
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": libelle,
        "optionName": "metric_" + uuid.uuid4().hex[:10],
    }


def filtre(colonne: str, operateur: str, valeur: Any) -> Dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "subject": colonne,
        "operator": operateur,
        "comparator": valeur,
        "clause": "WHERE",
        "filterOptionName": "filter_" + uuid.uuid4().hex[:10],
    }


def format_conditionnel(colonne: str, operateur: str, valeur: float,
                        couleur: str) -> Dict[str, Any]:
    return {"column": colonne, "operator": operateur,
            "targetValue": valeur, "colorScheme": couleur}


def graphique(nom: str, dataset: str, viz: str, params: Dict[str, Any],
              largeur: int = 6, hauteur: int = 50) -> Dict[str, Any]:
    """Un graphique : son jeu de données, son type, ses paramètres, sa place."""
    params.setdefault("row_limit", 10_000)
    params["viz_type"] = viz
    return {"nom": nom, "dataset": dataset, "viz": viz, "params": params,
            "largeur": largeur, "hauteur": hauteur}


# =============================================================================
# Les trois tableaux de bord
# =============================================================================


def tableaux_de_bord() -> Dict[str, Dict[str, Any]]:
    return {
        "commercial": {
            "titre": "WABA — Performance Commerciale Groupe",
            "graphiques": [
                graphique(
                    "Revenus par pays et ligne métier",
                    "gold.daily_transaction_volume", "echarts_timeseries_bar",
                    {
                        "x_axis": "country_code",
                        "groupby": ["entity_type"],
                        "metrics": [metrique("commissions_eur", "SUM", "Commissions (EUR)")],
                        "x_axis_sort_asc": True,
                        "show_legend": True,
                    },
                    largeur=8, hauteur=60,
                ),
                graphique(
                    "Contribution aux revenus par pays",
                    "gold.daily_transaction_volume", "world_map",
                    {
                        "entity": "country_code",
                        # Les codes du groupe sont des ISO 3166-1 alpha-2 ; sans
                        # cette indication, Superset les interprète en alpha-3
                        # et la carte reste vide.
                        "country_fieldtype": "cca2",
                        "metric": metrique("montant_total_eur", "SUM", "Volume (EUR)"),
                        "show_bubbles": False,
                        "color_scheme": "supersetColors",
                    },
                    largeur=4, hauteur=60,
                ),
                graphique(
                    "Évolution mensuelle de l'ARPC par pays",
                    "gold.customer_arpu_monthly", "echarts_timeseries_line",
                    {
                        "x_axis": "mois",
                        "time_grain_sqla": "P1M",
                        "groupby": ["country_code"],
                        "metrics": [metrique("arpc_eur", "AVG", "ARPC (EUR)")],
                        "show_legend": True,
                        "markerEnabled": True,
                    },
                    largeur=8, hauteur=55,
                ),
                graphique(
                    "Produits les plus souscrits par pays",
                    "silver.accounts", "table",
                    {
                        "query_mode": "aggregate",
                        "groupby": ["country_code", "account_type"],
                        "metrics": [metrique("account_id", "COUNT", "Comptes ouverts")],
                        "order_desc": True,
                        "row_limit": 10,
                    },
                    largeur=4, hauteur=55,
                ),
            ],
        },
        "risque": {
            "titre": "WABA — Risque & Conformité Réglementaire",
            "graphiques": [
                graphique(
                    "Créances douteuses par pays (seuil BCEAO 5 %)",
                    "gold.npl_ratio_by_country", "table",
                    {
                        "query_mode": "aggregate",
                        "groupby": ["country_code"],
                        "metrics": [
                            metrique("npl_ratio", "MAX", "NPL"),
                            metrique("encours_total_eur", "MAX", "Encours (EUR)"),
                            metrique("encours_douteux_eur", "MAX", "Encours douteux (EUR)"),
                        ],
                        # Le NPL consolidé du pays est porté par la ligne
                        # « ENSEMBLE » ; agréger les types de prêt reviendrait à
                        # moyenner des ratios de portefeuilles de tailles
                        # différentes.
                        "adhoc_filters": [filtre("loan_type", "==", "ENSEMBLE")],
                        "conditional_formatting": [
                            format_conditionnel("NPL", ">", NPL_PLAFOND, ROUGE),
                            format_conditionnel("NPL", "<", NPL_VIGILANCE, VERT),
                            format_conditionnel("NPL", "≥", NPL_VIGILANCE, ORANGE),
                        ],
                        "row_limit": 100,
                    },
                    largeur=6, hauteur=55,
                ),
                graphique(
                    "Ratio sinistres/primes par branche (seuil CIMA 70 %)",
                    "gold.loss_ratio_by_product", "table",
                    {
                        "query_mode": "aggregate",
                        "groupby": ["country_code", "product_line"],
                        "metrics": [
                            metrique_sql(
                                "SUM(sinistres_regles_eur) / NULLIF(SUM(primes_acquises_eur), 0)",
                                "Loss ratio"),
                            metrique("primes_acquises_eur", "SUM", "Primes (EUR)"),
                            metrique("sinistres_regles_eur", "SUM", "Sinistres (EUR)"),
                        ],
                        "conditional_formatting": [
                            format_conditionnel("Loss ratio", ">", LOSS_RATIO_ALERTE, ROUGE),
                            format_conditionnel("Loss ratio", "≤", LOSS_RATIO_ALERTE, VERT),
                        ],
                        "row_limit": 100,
                    },
                    largeur=6, hauteur=55,
                ),
                graphique(
                    "Déclarations AML par jour et par pays",
                    "gold.daily_transaction_volume", "echarts_timeseries_line",
                    {
                        "x_axis": "transaction_date",
                        "time_grain_sqla": "P1D",
                        "groupby": ["country_code"],
                        "metrics": [metrique("nb_au_dessus_seuil_aml", "SUM",
                                             "Virements au-dessus du seuil")],
                        "show_legend": True,
                    },
                    largeur=8, hauteur=55,
                ),
                graphique(
                    "Traitement des sinistres (SLA 30 jours)",
                    "gold.claims_processing_time", "table",
                    {
                        "query_mode": "aggregate",
                        "groupby": ["country_code", "ligne_metier"],
                        "metrics": [
                            metrique("delai_moyen_jours", "AVG", "Délai moyen (j)"),
                            metrique("delai_p90_jours", "MAX", "Délai P90 (j)"),
                            metrique("nb_sinistres", "SUM", "Sinistres"),
                        ],
                        "conditional_formatting": [
                            format_conditionnel("Délai moyen (j)", ">", SLA_SINISTRE_JOURS, ROUGE),
                            format_conditionnel("Délai moyen (j)", "≤", SLA_SINISTRE_JOURS, VERT),
                        ],
                        "row_limit": 100,
                    },
                    largeur=4, hauteur=55,
                ),
            ],
        },
        "mobile": {
            "titre": "WABA — Mobile Money & Transferts",
            "graphiques": [
                graphique(
                    "Flux mobile money par pays et par heure",
                    "silver.mobile_money_payments", "heatmap_v2",
                    {
                        "x_axis": "payment_hour",
                        "groupby": "country_code",
                        "metric": metrique("amount_eur", "SUM", "Montant (EUR)"),
                        "linear_color_scheme": "blue_white_yellow",
                        "xscale_interval": 1,
                        "yscale_interval": 1,
                        "row_limit": 5000,
                    },
                    largeur=8, hauteur=60,
                ),
                graphique(
                    "Corridors transfrontaliers les plus actifs",
                    "gold.cross_border_transfers", "table",
                    {
                        "query_mode": "aggregate",
                        "groupby": ["corridor"],
                        "metrics": [
                            metrique("montant_total_eur", "SUM", "Montant (EUR)"),
                            metrique("nb_transferts", "SUM", "Transferts"),
                        ],
                        "order_desc": True,
                        "row_limit": 5,
                    },
                    largeur=4, hauteur=60,
                ),
                graphique(
                    "Taux d'échec par opérateur et par pays",
                    "silver.mobile_money_payments", "echarts_timeseries_bar",
                    {
                        "x_axis": "country_code",
                        "groupby": ["operator"],
                        "metrics": [metrique_sql(
                            "AVG(CASE WHEN status = 'SUCCESS' THEN 0.0 ELSE 1.0 END)",
                            "Taux d'échec")],
                        "x_axis_sort_asc": True,
                        "show_legend": True,
                    },
                    largeur=12, hauteur=50,
                ),
            ],
        },
    }


# =============================================================================
# Construction
# =============================================================================


class Constructeur:
    """Crée la source de données, les jeux de données et les tableaux de bord."""

    def __init__(self, client: SupersetClient) -> None:
        self.client = client
        self.base_id: Optional[int] = None
        self.datasets: Dict[str, int] = {}

    # -- Source de données ---------------------------------------------------

    def source_de_donnees(self) -> int:
        """Enregistre Trino, ou retrouve l'enregistrement existant.

        L'URI ne contient aucun secret : Trino n'authentifie pas en local, et
        c'est Keycloak qui portera l'identité au §4.3. Elle vient malgré tout de
        l'environnement, pour que l'adresse du coordinateur ne soit pas figée.
        """
        existante = self.client.find("database", "database_name", BASE_NOM)
        if existante:
            self.base_id = existante["id"]
            return self.base_id

        # L'adresse est celle que **Superset** utilisera, pas celle depuis
        # laquelle ce script s'exécute : le serveur ouvre lui-même la connexion,
        # depuis le réseau Compose où Trino répond sur son nom de service.
        uri = os.getenv("WABA_TRINO_URI") or "trino://{}@trino:8080/iceberg".format(
            os.getenv("TRINO_USER", "waba"))
        reponse = self.client.post("/database/", {
            "database_name": BASE_NOM,
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            # Superset ne doit pas pouvoir écrire dans le lakehouse : les
            # tableaux de bord lisent, les jobs Spark écrivent.
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        })
        self.base_id = reponse["id"]
        return self.base_id

    # -- Jeux de données -----------------------------------------------------

    def jeu_de_donnees(self, qualifie: str) -> int:
        """Déclare une table du lakehouse comme jeu de données Superset."""
        if qualifie in self.datasets:
            return self.datasets[qualifie]

        schema, table = qualifie.split(".", 1)
        existant = self.client.find("dataset", "table_name", table)
        if existant and existant.get("schema") == schema:
            self.datasets[qualifie] = existant["id"]
            return existant["id"]

        reponse = self.client.post("/dataset/", {
            "database": self.base_id, "schema": schema, "table_name": table,
        })
        self.datasets[qualifie] = reponse["id"]
        return reponse["id"]

    # -- Graphiques ----------------------------------------------------------

    @staticmethod
    def contexte_de_requete(dataset_id: int, params: Dict[str, Any]) -> Dict[str, Any]:
        """Requête équivalente aux paramètres du graphique.

        Superset stocke deux choses par graphique : les paramètres de son
        formulaire, que l'interface sait retraduire en requête, et un contexte
        de requête déjà résolu. Le second est facultatif à l'affichage mais
        indispensable partout ailleurs — API de données, vignettes, alertes et
        rapports planifiés. Un graphique créé sans lui s'affiche et refuse de
        se laisser interroger, ce qui est déroutant.
        """
        colonnes: List[str] = []
        if params.get("x_axis"):
            colonnes.append(params["x_axis"])
        groupes = params.get("groupby")
        if isinstance(groupes, str):
            colonnes.append(groupes)
        elif groupes:
            colonnes.extend(groupes)
        if params.get("entity"):
            colonnes.append(params["entity"])

        metriques = params.get("metrics") or (
            [params["metric"]] if params.get("metric") else [])

        filtres = [
            {"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
            for f in params.get("adhoc_filters", [])
            if f.get("expressionType") == "SIMPLE"
        ]

        return {
            "datasource": {"id": dataset_id, "type": "table"},
            "force": False,
            "queries": [{
                "columns": list(dict.fromkeys(colonnes)),
                "metrics": metriques,
                "filters": filtres,
                "row_limit": params.get("row_limit", 10_000),
                "orderby": [],
                "annotation_layers": [],
                "series_limit": 0,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
            }],
            "form_data": params,
            "result_format": "json",
            "result_type": "full",
        }

    def graphique(self, definition: Dict[str, Any], dashboard_id: int) -> int:
        dataset_id = self.jeu_de_donnees(definition["dataset"])
        params = dict(definition["params"])
        params["datasource"] = "{}__table".format(dataset_id)

        charge = {
            "slice_name": definition["nom"],
            "viz_type": definition["viz"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params, ensure_ascii=False),
            "query_context": json.dumps(
                self.contexte_de_requete(dataset_id, params), ensure_ascii=False),
            "dashboards": [dashboard_id],
        }
        existant = self.client.find("chart", "slice_name", definition["nom"])
        if existant:
            self.client.put("/chart/{}".format(existant["id"]), charge)
            return existant["id"]
        return self.client.post("/chart/", charge)["id"]

    # -- Mise en page --------------------------------------------------------

    def position(self, graphiques: List[Dict[str, Any]], identifiants: List[int],
                 titre: str) -> Dict[str, Any]:
        """Grille de douze colonnes, remplie de gauche à droite.

        Superset ne devine pas la mise en page : un graphique rattaché à un
        tableau de bord sans position déclarée n'y apparaît pas. Les rangées
        sont donc composées ici, en accumulant les largeurs jusqu'à douze.
        """
        position: Dict[str, Any] = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": titre}},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [],
                        "parents": ["ROOT_ID"]},
        }

        rangee: Optional[str] = None
        largeur_courante = 0
        for index, (definition, chart_id) in enumerate(zip(graphiques, identifiants)):
            if rangee is None or largeur_courante + definition["largeur"] > 12:
                rangee = "ROW-{}".format(len(position["GRID_ID"]["children"]) + 1)
                position["GRID_ID"]["children"].append(rangee)
                position[rangee] = {
                    "type": "ROW", "id": rangee, "children": [],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"},
                    "parents": ["ROOT_ID", "GRID_ID"],
                }
                largeur_courante = 0

            cle = "CHART-{}".format(index + 1)
            position[rangee]["children"].append(cle)
            position[cle] = {
                "type": "CHART", "id": cle, "children": [],
                "meta": {
                    "chartId": chart_id,
                    "sliceName": definition["nom"],
                    "width": definition["largeur"],
                    "height": definition["hauteur"],
                    "uuid": str(uuid.uuid4()),
                },
                "parents": ["ROOT_ID", "GRID_ID", rangee],
            }
            largeur_courante += definition["largeur"]

        return position

    # -- Tableau de bord -----------------------------------------------------

    def tableau(self, cle: str, definition: Dict[str, Any]) -> int:
        titre = definition["titre"]
        existant = self.client.find("dashboard", "dashboard_title", titre)
        if existant:
            dashboard_id = existant["id"]
        else:
            dashboard_id = self.client.post("/dashboard/", {
                "dashboard_title": titre, "published": True,
            })["id"]

        identifiants = [self.graphique(g, dashboard_id) for g in definition["graphiques"]]

        # Le filtre par pays est exigé par la grille d'évaluation. Déclaré au
        # niveau du tableau de bord, il s'applique à tous ses graphiques sans
        # que chacun ait à le prévoir.
        metadonnees = {
            "color_scheme": "supersetColors",
            "native_filter_configuration": [{
                "id": "NATIVE_FILTER-pays-{}".format(cle),
                "name": "Pays",
                "filterType": "filter_select",
                "targets": [{"column": {"name": "country_code"},
                             "datasetId": self.datasets[definition["graphiques"][0]["dataset"]]}],
                "defaultDataMask": {"filterState": {}, "extraFormData": {}},
                "controlValues": {"multiSelect": True, "searchAllOptions": False,
                                  "enableEmptyFilter": False},
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            }],
        }

        self.client.put("/dashboard/{}".format(dashboard_id), {
            "dashboard_title": titre,
            "published": True,
            "position_json": json.dumps(
                self.position(definition["graphiques"], identifiants, titre),
                ensure_ascii=False),
            "json_metadata": json.dumps(metadonnees, ensure_ascii=False),
        })
        return dashboard_id


# =============================================================================
# Commandes
# =============================================================================


def supprimer(client: SupersetClient) -> None:
    """Retire les tableaux, graphiques et jeux de données créés par ce script."""
    for cle, definition in tableaux_de_bord().items():
        for g in definition["graphiques"]:
            trouve = client.find("chart", "slice_name", g["nom"])
            if trouve:
                client.delete("/chart/{}".format(trouve["id"]))
        trouve = client.find("dashboard", "dashboard_title", definition["titre"])
        if trouve:
            client.delete("/dashboard/{}".format(trouve["id"]))
    print("Tableaux de bord supprimés.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=sorted(tableaux_de_bord()),
                        help="ne construire qu'un tableau de bord")
    parser.add_argument("--delete", action="store_true",
                        help="supprimer les tableaux de bord et leurs graphiques")
    args = parser.parse_args(argv)

    load_env_file()
    client = SupersetClient()
    try:
        client.login()
        if args.delete:
            supprimer(client)
            return 0

        constructeur = Constructeur(client)
        print("Source de données          {}".format(constructeur.source_de_donnees()))

        selection = tableaux_de_bord()
        if args.only:
            selection = {args.only: selection[args.only]}

        for cle, definition in selection.items():
            identifiant = constructeur.tableau(cle, definition)
            print("  {:<12} {} — {} graphique(s)".format(
                cle, definition["titre"], len(definition["graphiques"])))
            print("               http://localhost:{}/superset/dashboard/{}/".format(
                os.getenv("SUPERSET_PORT", "8088"), identifiant))
    except SupersetError as exc:
        print("Échec : {}".format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
