# Journal des décisions d'architecture

Ce journal est alimenté **au fil de l'eau**, à la clôture de chaque niveau. Il constitue la matière
première du write-up technique de 2 à 5 pages attendu en livrable.

Format : contexte → décision → alternative écartée → conséquence assumée.

---

## J1 — Socle du Level 1

### D1. Catalogue Iceberg REST plutôt que Hive Metastore

**Contexte.** Le sujet laisse le choix entre un metastore Iceberg REST et un Hive Metastore (§1.4).

**Décision.** Catalogue REST (`apache/iceberg-rest-fixture`), métadonnées persistées dans un SQLite
monté sur volume.

**Alternative écartée.** Hive Metastore, qui aurait imposé un service Thrift **et** une base
PostgreSQL dédiée, soit environ 1,5 Go de RAM supplémentaires pour aucune fonctionnalité
supplémentaire dans ce périmètre.

**Conséquence assumée.** Le SQLite du catalogue est un point de contention en écriture concurrente.
Acceptable ici (un seul job Spark écrit à la fois) ; en production, on basculerait sur le backend
JDBC PostgreSQL du même service, sans changer une ligne de configuration côté Trino ou Spark.

### D2. Profils Compose exclusifs plutôt que cumulatifs

**Contexte.** La plateforme complète du Level 4 (~12 services, dont OpenMetadata et son
Elasticsearch) dépasse 24 Go de RAM. Le développement est mené sur une machine de 16 Go, sans
possibilité de recourir à une VM cloud.

**Décision.** Regroupement des services en profils Compose par niveau (`l1` … `l4`), avec une
`mem_limit` explicite sur chaque service, et plafonnement de WSL2 à 10 Go via `.wslconfig`.

**Alternative écartée.** Un unique `docker compose up` lançant l'intégralité de la plateforme.

**Conséquence assumée.** La démonstration vidéo sera un **montage de séquences enregistrées niveau
par niveau** et non une prise unique. Le critère « un seul `docker compose up` lance toute la
stack » du Level 1 reste satisfait à l'échelle du niveau concerné.

### D3. Credentials résolus à l'exécution

**Contexte.** Le sujet interdit explicitement tout credential codé en dur, y compris dans les
fichiers de configuration.

**Décision.** Les catalogues Trino utilisent la syntaxe `${ENV:VARIABLE}` ; les variables sont
fournies au conteneur par Compose depuis un `.env` local, lui-même exclu du dépôt et documenté par
`README.env.example`. Aucun fichier versionné ne contient de valeur sensible.

**Conséquence assumée.** Le dépôt n'est pas exécutable sans avoir d'abord copié
`README.env.example` en `.env` — étape rendue automatique par `make up-l1`.

### D4. Ports paramétrés, valeurs par défaut standard

**Contexte.** La machine de développement héberge déjà d'autres stacks qui occupent 9000, 9001 et
9092 — ports dont la plateforme a besoin (MinIO, puis Kafka au Level 3).

**Décision.** Tous les ports publiés sont des variables d'environnement, avec les **valeurs par
défaut attendues** (9000/9001/8080/8181) dans `README.env.example` pour que l'évaluateur retrouve un
environnement standard. Un `.env` local, non versionné, permet de les décaler sans toucher au dépôt
lorsqu'un port est déjà pris.

**Conséquence assumée.** Aucune. C'est le cas d'usage nominal d'un fichier `.env`.

### D5. Un test de fumée exécutable par niveau

**Contexte.** Les grilles d'évaluation du sujet sont des listes de critères binaires et vérifiables
(« re-lancer un job Spark ne génère pas de doublons », « les 8 tables `raw.*` existent dans Trino »).

**Décision.** Chaque niveau est accompagné d'un script (`scripts/smoke_l<n>.sh` / `.ps1`) qui vérifie
ses critères et sort en code non nul au premier échec. Un niveau n'est considéré comme clos que si
son test de fumée passe après un redémarrage complet depuis un état vierge.

**Conséquence assumée.** Coût initial d'écriture des scripts, largement amorti : ils servent de
contrôle de non-régression pendant les niveaux suivants et de trame à la vidéo de démonstration.

---

## Mesures

Budget mémoire relevé sur la machine de développement (Docker plafonné à 9,7 Go) :

| Niveau | Services | `mem_limit` cumulée | Consommation mesurée |
|---|---|---|---|
| Socle L1 | MinIO, Iceberg REST, Trino | 4,75 Go | **1,15 Go** |

La consommation réelle reste très en deçà des limites, qui jouent un rôle de garde-fou contre
l'emballement d'un composant (typiquement le heap de Trino) plutôt que de réservation.
