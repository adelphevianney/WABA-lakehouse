"""Amorçage du bucket en ligne de commande, sans interface.

L'application Streamlit couvre l'usage interactif exigé par l'énoncé, mais un
test de fumée ne peut pas cliquer sur des boutons. Ce point d'entrée génère le
même jeu de données par le même chemin de code (`service.run_batch`), ce qui
rend la vérification automatisable et reproductible.

Sert aussi à préparer une démonstration : un seul appel suffit à peupler les
huit pays avant d'enregistrer la vidéo.

Exemples :
    python -m generator.seed --preset demo
    python -m generator.seed --customers 500000 --accounts 800000 --rows 10000
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, time as dtime, timedelta

import numpy as np

from generator import config as cfg
from generator import service, storage

logger = logging.getLogger("generator.seed")

#: Volumétries prêtes à l'emploi. « demo » vise un test de fumée rapide,
#: « full » reprend les volumes et la période de l'annexe A.
#:
#: `rows` s'entend par fichier, donc par pays et par journée, et `days` fixe le
#: nombre de journées simulées. Le couple doit rester cohérent : étaler un
#: volume de démonstration sur un trimestre produirait quelques dizaines de
#: lignes par jour, une volumétrie qu'aucune banque ne connaît, et autant de
#: partitions Iceberg quasi vides.
PRESETS: dict[str, dict[str, int]] = {
    "demo": {"customers": 20_000, "accounts": 35_000, "branches": 200, "products": 50,
             "rows": 700, "days": 3},
    "full": {**cfg.REFERENTIAL_SIZES, "rows": 0, "days": 90},
}

#: Dernier jour simulé. La période remonte de `days` journées à partir de là.
LAST_DAY: date = date(2026, 3, 31)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m generator.seed",
        description="Génère référentiels et transactions, puis les dépose dans MinIO.",
    )
    parser.add_argument("--preset", choices=sorted(PRESETS), default="demo",
                        help="volumétrie prête à l'emploi (défaut : demo)")
    parser.add_argument("--customers", type=int, help="nombre de clients")
    parser.add_argument("--accounts", type=int, help="nombre de comptes")
    parser.add_argument("--rows", type=int,
                        help="lignes par fichier, donc par pays et par jour "
                             "(0 = valeurs par défaut de l'énoncé)")
    parser.add_argument("--days", type=int,
                        help="nombre de journées simulées, une par fichier")
    parser.add_argument("--countries", nargs="*", default=list(cfg.COUNTRY_CODES),
                        metavar="CC", help="pays à alimenter (défaut : les 8)")
    parser.add_argument("--kinds", nargs="*", default=list(service.KIND_LABELS),
                        metavar="TYPE", help="types de données à générer")
    parser.add_argument("--anomaly-rate", type=float, default=cfg.DEFAULT_ANOMALY_RATE,
                        help="taux d'anomalies injectées (défaut : %(default)s)")
    parser.add_argument("--reuse-referentials", action="store_true",
                        help="conserver les référentiels déjà présents, et ne les générer que s'ils "
                             "manquent — indispensable en exécution répétée, sinon les fichiers de "
                             "transactions déjà déposés référenceraient des clés disparues")
    parser.add_argument("--seed", type=int, default=None, help="graine du générateur aléatoire")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    preset = PRESETS[args.preset]

    sizes = {
        "customers": args.customers or preset["customers"],
        "accounts": args.accounts or preset["accounts"],
        "branches": preset["branches"],
        "products": preset["products"],
    }
    rows_per_file = args.rows if args.rows is not None else preset["rows"]
    days = max(args.days or preset["days"], 1)
    first_day = LAST_DAY - timedelta(days=days - 1)

    store = storage.RawLandingStore()
    reachable, message = store.is_reachable()
    if not reachable:
        logger.error("MinIO injoignable : %s", message)
        return 1

    started = time.perf_counter()

    if args.reuse_referentials and store.referentials_present():
        logger.info("réutilisation des référentiels déjà présents dans le bucket")
    else:
        logger.info("génération des référentiels %s", sizes)
        service.build_referentials(store, sizes, seed=args.seed)

    index = service.load_index(store)

    request = service.GenerationRequest(
        kinds=tuple(args.kinds),
        countries=tuple(args.countries),
        entity_types=(),
        rows={kind: rows_per_file for kind in args.kinds} if rows_per_file else {},
        start=datetime.combine(first_day, dtime.min),
        end=datetime.combine(LAST_DAY, dtime.max),
        anomaly_rate=args.anomaly_rate,
    )
    logger.info(
        "période simulée : %s au %s (%d jour(s)), %d fichier(s) attendu(s)",
        first_day, LAST_DAY, days, request.file_count(),
    )
    results = service.run_batch(index, store, request, np.random.default_rng(args.seed))

    produced = [r for r in results if r.ok]
    for result in (r for r in results if not r.ok):
        logger.info("ignoré — %s / %s : %s", result.country_code, result.kind, result.skipped_reason)

    logger.info(
        "%d fichiers, %s lignes, %d lignes altérées — en %.1f s",
        sum(r.files for r in produced),
        f"{sum(r.rows for r in produced):,}",
        sum(a.rows for r in produced for a in r.anomalies),
        time.perf_counter() - started,
    )
    return 0 if produced else 1


if __name__ == "__main__":
    sys.exit(main())
