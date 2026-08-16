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
import urllib.request
from typing import Any, Dict, Optional

BASE_URL = os.getenv("NIFI_URL", "https://localhost:8091/nifi-api")

# Le certificat est auto-signé et régénéré à chaque création du conteneur : il
# n'existe aucune autorité à laquelle le rattacher.
_SSL_CONTEXT = ssl.create_default_context()
_SSL_CONTEXT.check_hostname = False
_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


class NiFiError(RuntimeError):
    pass


class NiFiClient:
    """Appels authentifiés à l'API NiFi."""

    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self._token: Optional[str] = None

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

    # -- Raccourcis ----------------------------------------------------------

    def root_process_group_id(self) -> str:
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]


def main() -> int:
    import urllib.parse  # noqa: F401 — utilisé par login()

    client = NiFiClient()
    try:
        client.login()
        about = client.get("/flow/about")["about"]
        racine = client.root_process_group_id()
    except NiFiError as exc:
        print("Connexion à NiFi impossible : {}".format(exc))
        return 1

    print("Connecté à NiFi {} — groupe racine {}".format(about["version"], racine))
    types = {t["type"].rsplit(".", 1)[-1] for t in client.get("/flow/processor-types")["processorTypes"]}
    requis = {"ListS3", "FetchS3Object", "SplitRecord", "UpdateRecord", "PublishKafkaRecord"}
    presents = {p for p in requis if any(p == t or t.startswith(p) for t in types)}
    print("Processeurs requis disponibles : {}".format(", ".join(sorted(presents)) or "aucun"))
    manquants = requis - presents
    if manquants:
        print("Manquants : {}".format(", ".join(sorted(manquants))))
    return 0


if __name__ == "__main__":
    import urllib.parse
    sys.exit(main())
