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

## J2 — Générateur de données

### D6. La matrice pays × entité prime sur le script de référence

**Contexte.** Le tableau des entités de l'énoncé décrit une implantation
différenciée : le mobile money n'opère qu'en Côte d'Ivoire, au Sénégal, au Burkina Faso
et au Ghana ; la microfinance qu'au Mali, en Guinée et au Burkina Faso. Le script fourni
en annexe A.8 tire pourtant `entity_type` uniformément sur les huit pays.

**Décision.** La matrice est encodée dans `generator/config.py` et appliquée à la
génération. Les poids d'entité sont renormalisés pays par pays : un marché sans mobile
money redistribue ce poids sur les entités réellement présentes.

**Alternative écartée.** Suivre le script de référence, plus simple mais produisant du
microcrédit au Ghana et des portefeuilles mobiles au Mali.

**Conséquence assumée.** L'interface refuse de générer des paiements mobile money au
Mali, avec un message métier explicite plutôt qu'une erreur technique.

### D7. Calibrer d'abord, générer ensuite

**Contexte.** L'énoncé impose un NPL entre 3 % et 8 % et un loss ratio entre 50 % et
85 %. Ces ratios sont des propriétés d'ensemble : aucun réglage des probabilités ligne à
ligne ne les garantit. Un tirage indépendant des primes et des sinistres produit
couramment des loss ratios de plusieurs centaines de pour cent.

**Décision.** Les cibles sont fixées par pays (et par branche pour l'assurance), puis la
génération est pilotée pour les atteindre : sélection déterministe des comptes en défaut
par hachage de l'identifiant, et dimensionnement des sinistres à partir de l'enveloppe de
primes réellement encaissée dans le lot.

**Alternative écartée.** Ajuster empiriquement les paramètres jusqu'à tomber dans la
fourchette — non reproductible, et invalidé au moindre changement de volumétrie.

**Conséquence assumée.** Le NPL réalisé, pondéré par les encours, s'écarte de sa cible de
quelques dixièmes de point. Les cibles visent donc l'intérieur de la fourchette. Mesuré à
pleine volumétrie : écart maximal de 0,68 point, huit pays sur huit conformes.

### D8. Injecter les anomalies sans les étiqueter

**Contexte.** Les trois règles de fraude du Level 3 ne se déclenchent jamais sur des
données aléatoires : une rafale de virements depuis un même compte en moins de cinq
minutes a une probabilité nulle quand chaque ligne tire son compte indépendamment.
Mesuré avant injection : zéro détection sur les trois règles.

**Décision.** Un module d'injection dédié, à taux paramétrable, réécrit des lignes
existantes — sans ajouter de colonne indicatrice, qui donnerait la réponse au pipeline de
détection et ferait diverger le schéma de l'annexe. Le même module expose les
**détecteurs de référence**, qui servent d'oracle aux tests et que les jobs Spark
Structured Streaming du Level 3 devront reproduire.

**Conséquence assumée.** Le taux d'anomalies est un paramètre de simulation, pas une
propriété du réel. Il est affiché dans l'interface et journalisé à chaque lot.

### D9. Prime annuelle attachée à la police

**Contexte.** La règle « sinistre supérieur à trois fois la prime annuelle » suppose un
montant de référence par police. Dans une génération opération par opération, un sinistre
valait en moyenne 3,8 fois une prime : la règle se serait déclenchée sur presque tout.

**Décision.** Chaque police porte une prime annuelle déterministe ; les versements en sont
des échéances mensuelles et les sinistres une fraction, calibrée par le loss ratio cible.

**Conséquence assumée.** Mesuré après ce changement : sinistre médian à 0,27 fois la prime
annuelle, maximum 2,43 — aucun ne franchit naturellement le seuil de 3, qui redevient donc
discriminant.

### D10. Génération vectorisée

**Contexte.** Le script de l'annexe A.8 appelle `np.random.choice` ligne par ligne.

**Décision.** Raisonner par colonnes : tirages groupés par pays, construction des
identifiants par opérations vectorisées.

**Conséquence assumée.** Les 500 000 clients et 800 000 comptes de l'annexe sont produits
en 4,5 secondes, ce qui rend la démonstration possible à pleine volumétrie plutôt que sur
un échantillon.

### D11. Champs personnels absents des schémas

**Contexte.** Les contraintes transverses imposent de masquer « IBAN, numéros de compte »
et le Level 4 exige de taguer `iban` et `account_number` comme données personnelles. Or
aucun schéma de l'annexe ne comporte ces champs.

**Décision.** Les deux colonnes sont ajoutées au référentiel comptes, avec un IBAN au
format plausible — **nul au Ghana**, qui ne fait pas partie du registre IBAN. Ce null
n'est pas une donnée manquante mais une réalité métier, et il donne à la couche Silver un
vrai cas de gestion des valeurs nulles.

**Conséquence assumée.** Les couches brutes conservent la valeur en clair, comme toute
zone d'atterrissage ; la pseudonymisation est appliquée à partir de la couche Silver, où
elle est démontrable en SQL. L'accès aux couches amont sera restreint par les rôles
Keycloak du Level 4.

---

## Mesures

Budget mémoire relevé sur la machine de développement (Docker plafonné à 9,7 Go) :

| Niveau | Services | `mem_limit` cumulée | Consommation mesurée |
|---|---|---|---|
| Socle L1 | MinIO, Iceberg REST, Trino | 4,75 Go | **1,15 Go** |
| L1 complet | + générateur Streamlit | 7,75 Go | **1,19 Go** au repos |

La consommation réelle reste très en deçà des limites, qui jouent un rôle de garde-fou contre
l'emballement d'un composant (typiquement le heap de Trino) plutôt que de réservation.

Performance du générateur, à la volumétrie de l'annexe A :

| Étape | Volume | Durée |
|---|---|---|
| Référentiels complets | 500 000 clients + 800 000 comptes | 4,5 s |
| Lot transactionnel | 240 000 lignes sur 8 pays | 0,9 s |
| Suite de tests | 101 tests | 2,5 s |
