"""Adaptateur Prometheus pour NiFi (Level 4.4).

NiFi 2 expose ses métriques au format Prometheus sur
`/nifi-api/flow/metrics/prometheus`, mais derrière la même authentification que
le reste de son API — et il a retiré la tâche de rapport qui, en 1.x, ouvrait un
port dédié sans authentification. Prometheus, lui, ne sait présenter qu'un
jeton statique lu dans un fichier, alors que celui de NiFi expire.

Cet adaptateur comble l'écart : il maintient un jeton valide, le renouvelle
quand NiFi le refuse, et sert les métriques sans authentification sur le réseau
interne de la plateforme. Une trentaine de lignes valent mieux qu'un jeton
recopié à la main dans un fichier toutes les douze heures.

Usage :
    python scripts/nifi_exporter.py            # écoute sur le port 9103
"""

from __future__ import annotations

import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.nifi_client import NiFiClient, NiFiError, load_env_file  # noqa: E402

logger = logging.getLogger("nifi.exporter")

PORT = int(os.getenv("NIFI_EXPORTER_PORT", "9103"))
CHEMIN_NIFI = "/flow/metrics/prometheus"

_client = NiFiClient()


def metriques() -> str:
    """Métriques NiFi, en renouvelant le jeton s'il a expiré."""
    for tentative in (1, 2):
        try:
            requete = _client.request_brut("GET", CHEMIN_NIFI)
            return requete
        except NiFiError as exc:
            # Un jeton expiré se manifeste par un 401 : on se réauthentifie une
            # fois avant d'abandonner, plutôt qu'à chaque appel.
            if tentative == 1 and "401" in str(exc):
                logger.info("jeton expiré, réauthentification")
                _client.login()
                continue
            raise
    return ""


class Adaptateur(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — imposé par la bibliothèque standard
        if self.path.rstrip("/") not in ("/metrics", ""):
            self.send_error(404)
            return
        try:
            corps = metriques().encode("utf-8")
        except NiFiError as exc:
            logger.warning("NiFi injoignable : %s", exc)
            # 503 plutôt qu'une réponse vide : Prometheus marque la cible comme
            # indisponible, ce qui est l'information juste.
            self.send_error(503, "NiFi injoignable")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(corps)))
        self.end_headers()
        self.wfile.write(corps)

    def log_message(self, *args) -> None:
        """Silence : Prometheus interroge toutes les quinze secondes."""


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    load_env_file()
    try:
        _client.login()
    except NiFiError as exc:
        # L'adaptateur démarre quand même : NiFi met plus longtemps à être prêt
        # que lui, et une première tentative infructueuse n'est pas une panne.
        logger.warning("authentification initiale impossible (%s), on réessaiera", exc)

    logger.info("adaptateur NiFi à l'écoute sur le port %d", PORT)
    ThreadingHTTPServer(("0.0.0.0", PORT), Adaptateur).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
