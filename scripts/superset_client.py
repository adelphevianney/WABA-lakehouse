"""Client de l'API Superset, et sonde de connexion.

Même principe que `scripts/nifi_client.py` : les tableaux de bord sont construits
par appels d'API plutôt que cliqués puis exportés. Un export Superset est une
archive de plusieurs milliers de lignes de YAML généré, où l'ajout d'un
graphique produit un diff illisible et où les identifiants changent à chaque
export. Un script se relit, se compare d'une version à l'autre, et se rejoue à
l'identique sur une installation vierge.

L'authentification se fait en deux temps : un jeton JWT pour l'autorisation, et
un jeton CSRF pour toute écriture. Le second voyage avec un cookie de session,
d'où le gestionnaire de cookies — sans lui, Superset accepte le jeton CSRF puis
refuse la requête, ce qui est déroutant.

Les identifiants proviennent de l'environnement, jamais du code.

Usage :
    python scripts/superset_client.py        # vérifie la connexion
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any, Dict, Optional

from scripts.nifi_client import load_env_file  # même lecture de `.env`

BASE_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")


class SupersetError(RuntimeError):
    pass


class SupersetClient:
    """Appels authentifiés à l'API REST de Superset."""

    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/") + "/api/v1"
        self._token: Optional[str] = None
        self._csrf: Optional[str] = None
        # Superset lie le jeton CSRF à une session : les deux doivent voyager
        # ensemble, sinon l'écriture est refusée sans explication utile.
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(CookieJar())
        )

    # -- Authentification ----------------------------------------------------

    def login(self) -> None:
        username = os.getenv("SUPERSET_ADMIN", "admin")
        password = os.getenv("SUPERSET_ADMIN_PASSWORD")
        if not password:
            raise SupersetError(
                "Mot de passe Superset absent : définir SUPERSET_ADMIN_PASSWORD. "
                "Voir README.env.example."
            )

        reponse = self._call("POST", "/security/login", {
            "username": username, "password": password,
            "provider": "db", "refresh": True,
        }, authentifie=False)
        self._token = reponse["access_token"]
        self._csrf = self._call("GET", "/security/csrf_token/", None)["result"]

    def _headers(self, authentifie: bool = True) -> Dict[str, str]:
        entetes = {"Content-Type": "application/json", "Accept": "application/json"}
        if authentifie and self._token:
            entetes["Authorization"] = "Bearer {}".format(self._token)
        if self._csrf:
            entetes["X-CSRFToken"] = self._csrf
            entetes["Referer"] = self.base_url
        return entetes

    # -- Verbes --------------------------------------------------------------

    def _call(self, method: str, path: str, payload: Optional[Dict[str, Any]],
              authentifie: bool = True) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        requete = urllib.request.Request(
            self.base_url + path, data=data, method=method,
            headers=self._headers(authentifie),
        )
        try:
            with self._opener.open(requete, timeout=120) as reponse:
                corps = reponse.read().decode()
                return json.loads(corps) if corps else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode()[:400]
            raise SupersetError("{} {} → {} : {}".format(method, path, exc.code, detail)) from exc
        except urllib.error.URLError as exc:
            raise SupersetError(
                "Superset injoignable sur {} : {}. Le service est-il démarré "
                "(profil l4) ?".format(self.base_url, exc.reason)
            ) from exc

    def get(self, path: str, **params: Any) -> Any:
        if params:
            path += "?" + urllib.parse.urlencode(params)
        return self._call("GET", path, None)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._call("POST", path, payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._call("PUT", path, payload)

    def delete(self, path: str) -> Any:
        return self._call("DELETE", path, None)

    # -- Recherche -----------------------------------------------------------

    def find(self, ressource: str, colonne: str, valeur: str) -> Optional[Dict[str, Any]]:
        """Premier objet dont `colonne` vaut `valeur`, ou None.

        Superset n'expose pas de recherche par nom exact : le filtre `eq` de
        l'API Rison est le plus proche, et il suffit ici puisque les noms sont
        choisis par ce script.
        """
        # L'API attend du Rison, pas du JSON : une syntaxe compacte propre à
        # Flask-AppBuilder, où les listes s'écrivent `!(...)` et les chaînes
        # entre apostrophes.
        # En Rison, le point d'exclamation est le caractère d'échappement et
        # l'apostrophe délimite les chaînes : « Taux d'échec » couperait le
        # filtre en deux sans cette précaution.
        echappe = str(valeur).replace("!", "!!").replace("'", "!'")
        rison = "(filters:!((col:{},opr:eq,value:'{}')))".format(colonne, echappe)
        # Les noms comportent des espaces et des accents : sans encodage, la
        # bibliothèque standard refuse l'URL avant même de l'envoyer.
        resultat = self._call(
            "GET",
            "/{}/?q={}".format(ressource, urllib.parse.quote(rison, safe="")),
            None,
        )
        elements = resultat.get("result") or []
        if not elements:
            return None
        premier = dict(elements[0])
        premier.setdefault("id", (resultat.get("ids") or [None])[0])
        return premier


def main() -> int:
    load_env_file()
    client = SupersetClient()
    try:
        client.login()
        bases = client.get("/database/")
    except SupersetError as exc:
        print("Connexion à Superset impossible : {}".format(exc))
        return 1

    print("Connecté à Superset — {} source(s) de données enregistrée(s)".format(
        bases.get("count", 0)))
    for base in bases.get("result", []):
        print("  {}".format(base.get("database_name")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
