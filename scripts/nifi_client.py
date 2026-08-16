"""Client minimal de l'API NiFi, et sonde de connexion.

NiFi 2 a supprimé l'écoute HTTP en clair : le serveur démarre en HTTPS avec un
certificat auto-signé et une authentification mono-utilisateur. Un client d'API
doit donc s'authentifier pour obtenir un jeton, puis le présenter à chaque appel.

Le certificat étant auto-signé et généré au démarrage du conteneur, sa
vérification est désactivée — dans un déploiement réel, on lui substituerait un
certificat émis par une autorité reconnue, ce que fera le Level 4.

Les identifiants proviennent de l'environnement, jamais du code.

Usage :
    python scripts/nifi_client.py            # vérifie la connexion
"""

from __future__ import annotations

import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

BASE_URL = os.getenv("NIFI_URL", "https://localhost:8091/nifi-api")

#: Racine du dépôt, d'où provient le fichier `.env`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Le certificat est auto-signé et régénéré à chaque création du conteneur : il
# n'existe aucune autorité à laquelle le rattacher.
_SSL_CONTEXT = ssl.create_default_context()
_SSL_CONTEXT.check_hostname = False
_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


class NiFiError(RuntimeError):
    pass


def load_env_file(path: Optional[Path] = None) -> None:
    """Charge `.env` dans l'environnement, sans écraser ce qui est déjà défini.

    Les scripts NiFi s'exécutent sur la machine hôte, hors de Compose : sans
    cela il faudrait exporter à la main les identifiants que `.env` contient
    déjà, et la tentation serait grande de les écrire dans le script.
    """
    env_file = path or PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


class NiFiClient:
    """Appels authentifiés à l'API NiFi."""

    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self._token: Optional[str] = None
        # NiFi trace l'auteur de chaque modification ; un identifiant stable par
        # exécution rend l'historique du canevas lisible.
        self.client_id = "waba-flow-{}".format(uuid.uuid4())

    # -- Authentification ----------------------------------------------------

    def login(self) -> str:
        username = os.getenv("NIFI_USERNAME")
        password = os.getenv("NIFI_PASSWORD")
        if not username or not password:
            raise NiFiError(
                "Identifiants NiFi absents : définir NIFI_USERNAME et NIFI_PASSWORD. "
                "Voir README.env.example."
            )

        body = urllib.parse.urlencode({"username": username, "password": password}).encode()
        request = urllib.request.Request(
            self.base_url + "/access/token",
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(request, context=_SSL_CONTEXT, timeout=30) as response:
                self._token = response.read().decode()
        except urllib.error.HTTPError as exc:
            raise NiFiError(
                "authentification refusée ({}) — le mot de passe fait-il au moins "
                "12 caractères ?".format(exc.code)
            ) from exc
        except urllib.error.URLError as exc:
            raise NiFiError(
                "NiFi injoignable sur {} : {}. Le service est-il démarré "
                "(profil l3) ?".format(self.base_url, exc.reason)
            ) from exc
        return self._token

    def _headers(self) -> Dict[str, str]:
        if self._token is None:
            self.login()
        return {"Authorization": "Bearer {}".format(self._token),
                "Content-Type": "application/json"}

    # -- Verbes --------------------------------------------------------------

    def request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            self.base_url + path, data=data, method=method, headers=self._headers()
        )
        try:
            with urllib.request.urlopen(req, context=_SSL_CONTEXT, timeout=60) as response:
                body = response.read().decode()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode()[:300]
            raise NiFiError("{} {} → {} : {}".format(method, path, exc.code, detail)) from exc

    def get(self, path: str) -> Any:
        return self.request("GET", path)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self.request("POST", path, payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Any:
        return self.request("PUT", path, payload)

    def delete(self, path: str, version: int) -> Any:
        """Suppression d'un composant, que NiFi conditionne à sa révision.

        C'est le verrouillage optimiste de l'API : une révision périmée signifie
        qu'un autre client a modifié le composant entre-temps, et la suppression
        est refusée plutôt qu'appliquée à l'aveugle.
        """
        separator = "&" if "?" in path else "?"
        query = "version={}&clientId={}".format(version, self.client_id)
        return self.request("DELETE", "{}{}{}".format(path, separator, query))

    # -- Raccourcis ----------------------------------------------------------

    def new_revision(self) -> Dict[str, Any]:
        return {"version": 0, "clientId": self.client_id}

    def revision_of(self, path: str) -> Dict[str, Any]:
        entity = self.get(path)
        return {"version": entity["revision"]["version"], "clientId": self.client_id}

    def root_process_group_id(self) -> str:
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]


def main() -> int:
    load_env_file()
    client = NiFiClient()
    try:
        client.login()
        about = client.get("/flow/about")["about"]
        racine = client.root_process_group_id()
    except NiFiError as exc:
        print("Connexion à NiFi impossible : {}".format(exc))
        return 1

    print("Connecté à NiFi {} — groupe racine {}".format(about["version"], racine))

    # NiFi 2 a fusionné `PublishKafkaRecord` dans `PublishKafka` : les noms
    # employés par l'énoncé sont ceux de NiFi 1.x, et les chercher tels quels
    # mène à conclure à tort que le paquet Kafka est absent.
    types = {t["type"] for t in client.get("/flow/processor-types")["processorTypes"]}
    requis = {
        "org.apache.nifi.processors.aws.s3.ListS3",
        "org.apache.nifi.processors.aws.s3.FetchS3Object",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "org.apache.nifi.processors.standard.SplitRecord",
        "org.apache.nifi.processors.standard.UpdateRecord",
        "org.apache.nifi.kafka.processors.PublishKafka",
    }
    manquants = requis - types
    print("Processeurs requis : {} / {} disponibles".format(
        len(requis) - len(manquants), len(requis)))
    if manquants:
        print("Manquants : {}".format(", ".join(sorted(manquants))))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
