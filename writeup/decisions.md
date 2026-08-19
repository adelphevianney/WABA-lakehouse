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

## J3 — Jobs Spark d'ingestion

### D12. Spark en mode local, jars intégrés à l'image

**Contexte.** L'ingestion porte sur des lots de quelques dizaines de milliers à
quelques millions de lignes, sur une machine de 16 Go.

**Décision.** Spark tourne en `local[*]` dans un conteneur unique. Les quatre jars
nécessaires — `iceberg-spark-runtime`, `iceberg-aws-bundle`, `hadoop-aws`,
`aws-java-sdk-bundle` — sont intégrés à l'image plutôt que résolus par
`--packages` au lancement.

**Alternative écartée.** Un cluster autonome master + worker, qui aurait coûté
3 Go de mémoire supplémentaires sans rien apporter à cette échelle.

**Conséquence assumée.** Le job démarre sans accès réseau et sans latence de
résolution Maven, mais l'image pèse environ 1 Go. Deux piles d'accès au stockage
cohabitent et il faut le savoir : Iceberg écrit via son propre `S3FileIO`, tandis
que la lecture des CSV passe par le système de fichiers Hadoop `s3a`. Configurer
l'une ne configure pas l'autre — une session à laquelle il manque la seconde
écrit correctement dans le lakehouse mais ne sait pas lire sa source.

### D13. Lire en texte, typer ensuite

**Contexte.** Le §1.3 demande de rejeter les lignes malformées. Laisser Spark
convertir à la lecture transforme toute valeur invalide en `null`, sans
distinction possible entre une donnée absente et une donnée corrompue.

**Décision.** Le schéma de lecture est entièrement textuel ; la conversion est
explicite et postérieure à la validation. Une valeur présente mais non
convertible est identifiée comme telle et rejetée avec son motif.

**Conséquence assumée.** Les lignes écartées sont conservées dans une table
`raw.ingestion_rejects` avec leur motif et leur fichier d'origine. Sans elle,
tout écart de volumétrie entre la source et la cible serait inexplicable. Par
ailleurs, l'option `enforceSchema=false` fait vérifier l'en-tête du fichier
contre le schéma : par défaut, Spark apparie les colonnes **par position** et un
fichier réordonné serait ingéré de travers, silencieusement.

### D14. Idempotence par MERGE, sous trois conditions

**Contexte.** Critère d'évaluation explicite : rejouer un job sur un même fichier
ne doit pas créer de doublons.

**Décision.** `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` sur la clé naturelle.
Trois précautions rendent la garantie effective, et aucune n'est optionnelle :

1. **Déduplication de la source avant le MERGE.** Iceberg refuse qu'une ligne
   cible soit appariée plusieurs fois, mais deux lignes identiques d'un même lot
   ne correspondent à *aucune* ligne cible : elles seraient toutes deux insérées.
2. **Format de table v2.** Le MERGE s'appuie sur les suppressions au niveau
   ligne ; une table v1 échouerait.
3. **`country_code` dans le prédicat d'appariement.** C'est une colonne de
   partition : sans elle, chaque lot balaierait l'intégralité de la table.

**Vérification.** Le test de fumée redépose un jeu de données strictement
identique — même graine, donc mêmes identifiants — sous de nouveaux noms de
fichiers, réingère, et compare les comptages : 111 250 lignes avant, 111 250
après.

### D15. Ingestion par lots bornés

**Contexte.** Une première exécution sur une zone d'atterrissage ayant accumulé
816 fichiers a fait tomber le driver Spark.

**Décision.** Les fichiers sont traités par lots de 150 au maximum, paramétrable.
Le coût mémoire devient indépendant du retard accumulé.

**Conséquence assumée.** Plusieurs passes de lecture et de MERGE au lieu d'une,
pour un gain de robustesse qui compte davantage : une zone d'atterrissage en
retard est une situation d'exploitation normale, pas un cas limite.

### D16. Les référentiels ne sont pas archivés

**Contexte.** Le §1.2 demande de déplacer les fichiers traités vers le bucket
d'archive. Appliqué aux référentiels, ce déplacement a produit un effet de bord
que le test d'idempotence a révélé : le générateur, ne les trouvant plus, en
recréait de nouveaux, et toutes les clés des transactions déjà déposées
devenaient orphelines.

**Décision.** Seuls les fichiers transactionnels sont archivés. Les référentiels
sont des données de référence partagées, pas un flux consommé une fois ; ils
restent disponibles et sont réingérés à chaque passe, sans effet grâce au MERGE.

**Conséquence assumée.** Quelques dizaines de milliers de lignes relues à chaque
exécution, pour un coût négligeable et une cohérence préservée.

### D17. Un fichier par journée, et compaction séparée

**Contexte.** Après la première ingestion complète, la table `bank_transactions`
comptait **720 partitions pour 16 000 lignes**, soit 22 lignes et un fichier Parquet de
7 Ko par partition. Comparée au référentiel `accounts`, écrit en fichiers pleins, la
même donnée occupait **308 octets par ligne contre 29** — un facteur dix.

**Diagnostic.** Le partitionnement `days(timestamp)` n'était pas en cause : c'est le
bon choix pour une banque traitant des millions d'opérations par jour, et l'élagage par
jour est précisément ce qu'on en attend. La cause racine était la **forme des données
générées** : un fichier unique par pays couvrait un trimestre entier, soit 22
transactions par jour et par pays. Aucun système source ne produit cela — et la
nomenclature imposée par l'énoncé, `bank_txn_CI_20260101_01.csv`, désigne bien le
**jour** des transactions contenues. On respectait la forme du nom, pas son sens.

Le morcellement coûte sur trois plans : Parquet ne peut amortir ni ses dictionnaires ni
ses statistiques de colonnes sur quelques dizaines de lignes ; sur un stockage objet,
chaque fichier est une requête HTTP dont la latence domine complètement le temps de
transfert ; et Iceberg référence chaque fichier dans ses manifestes, relus à chaque
planification de requête.

**Décision.** Deux mesures complémentaires.

1. Le générateur produit **un fichier par pays et par journée**, aligné sur la
   nomenclature. Le préréglage `demo` couvre trois jours ; l'interface Streamlit
   conserve le dernier trimestre par défaut, exigé par le §1.1, et annonce désormais le
   nombre de fichiers avant de lancer.
2. Un job de compaction dédié, `jobs.batch.compact`, fusionne les petits fichiers via
   `rewrite_data_files`. Il reste nécessaire quelle que soit la forme des données : des
   ingestions successives dans les mêmes partitions y ajoutent chacune leurs fichiers
   sans réécrire les précédents. Le séparer de l'ingestion sépare deux responsabilités
   aux rythmes distincts ; au Level 2 il deviendra un DAG de maintenance.

**Alternative écartée.** Passer à `months()`. Cela aurait masqué le symptôme sans
traiter la cause, et éloigné le partitionnement de ce qu'on ferait en production.

**Conséquence assumée.** Une demande portant sur un trimestre produit 90 fichiers par
pays et par type. C'est volumineux, mais conforme à ce que décrit l'énoncé.

**Effet de bord découvert.** Le contrôle du partitionnement a révélé un défaut du
générateur : l'injection de rafales de fraude décale les horodatages vers l'avant et,
partie d'une transaction proche de minuit, débordait sur le lendemain. Cinq lignes d'un
fichier daté du 31 mars portaient un horodatage du 1er avril, créant deux partitions
parasites. Le point de départ d'une rafale est désormais borné, et un test le vérifie.

### D18. Volumétrie minimale du référentiel de démonstration

**Contexte.** Le NPL est pondéré par les encours, dont la distribution lognormale est
très dispersée. Sur un référentiel de 35 000 comptes, soit quelques centaines de prêts
par pays, deux ou trois gros défauts déplacent le ratio de plusieurs points : mesuré sur
six graines, **la moitié des pays sortait de la fourchette réglementaire 3-8 %**.

**Décision.** Le préréglage `demo` passe à 250 000 comptes, et la marge de sécurité des
cibles de 0,006 à 0,010. Mesuré sur 48 combinaisons (6 graines × 8 pays) : **aucun pays
hors fourchette**, NPL réalisé entre 3,24 % et 7,09 %. Un référentiel de 400 000 comptes
n'apportait pas de fiabilité supplémentaire pour 60 % de données en plus.

**Conséquence assumée.** Le test de fumée passe d'environ 150 à 234 secondes. Une
démonstration dont l'indicateur phare sort de sa fourchette ne démontre rien : le coût
est justifié.

### D19. Le NPL est un indicateur de stock, pas de flux — à trancher au Level 2

**Constat.** Calculé en SQL depuis les remboursements observés sur trois jours, le NPL
retombe entre 1,06 % et 3,44 %, sous la fourchette — alors que la calibration au niveau
des comptes, elle, est conforme. La cause n'est pas la calibration mais **la fenêtre
d'observation** : seuls **36,9 %** des prêts du portefeuille ont une échéance dans ces
trois jours.

Ce n'est pas un artefact de simulation, c'est la réalité métier : un prêt est remboursé
par échéances mensuelles, donc trois jours n'en font apparaître qu'une fraction. Un prêt
en défaut dont l'échéance tombe le 15 est invisible dans un fichier du 3.

**Ce que cela implique.** Le NPL est un **indicateur de stock**, mesuré sur l'intégralité
du portefeuille à une date donnée, et non un ratio calculé sur le flux d'une période
courte. Le job Gold du Level 2 devra donc :

* prendre pour dénominateur **tous** les comptes de prêt du référentiel, pas seulement
  ceux vus dans la période ;
* déterminer le statut de chaque prêt sur un historique suffisamment long — au moins un
  cycle d'échéances, soit un mois — plutôt que sur la seule fenêtre de traitement ;
* considérer comme sain, faute d'élément contraire, un prêt sans impayé observé.

**Décision reportée.** Corriger cela maintenant reviendrait à écrire la logique Gold
avant la couche Silver dont elle dépend. Le point est consigné ici pour être traité à sa
place, lors de la conception de `gold.npl_ratio_by_country`.

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
| Suite de tests | 159 tests | 4,2 s |

Ingestion Spark vers Iceberg, en mode local sur 6 vCPU :

| Étape | Volume | Durée |
|---|---|---|
| Ingestion complète des 8 tables | 84 fichiers, 113 800 lignes | ~90 s |
| Un seul jeu de données | 102 fichiers, 1 004 000 lignes | 29 s |
| Réingestion à l'identique | lignes relues, 0 insérée | ~90 s |

Effet du découpage journalier et de la compaction sur `raw.bank_transactions` :

| État | Partitions | Fichiers | Lignes/partition | Octets/ligne |
|---|---:|---:|---:|---:|
| Un fichier par trimestre | 720 | 720 | 22 | **308** |
| Un fichier par journée | 24 | 24 | 700 | **43** |
| Après 4 ingestions successives | 24 | 96 | 4 200 | 46 |
| Après compaction | 24 | **24** | 4 200 | **40** |

---

## J4 — Level 2 : médaillon et orchestration

### D20. Bronze exposée en vues plutôt que dupliquée

**Contexte.** Le §1.3 impose des tables `raw.*` et le §2.2 décrit une zone `bronze.*`
avec exactement la même définition. Ce sont deux noms pour une seule zone.

**Décision.** `bronze.*` est un ensemble de vues sur `raw.*`. Les deux vocabulaires de
l'énoncé fonctionnent, sans dédoubler la donnée.

**Alternative écartée.** Recopier physiquement les tables : double stockage, et deux
copies censées être identiques qui peuvent diverger.

**Conséquence assumée.** Les vues sont créées **par Trino** et non par Spark : une vue
Iceberg mémorise le dialecte SQL qui l'a produite, et Trino refuse de lire celles
écrites par Spark — « Cannot read unsupported dialect 'spark' ». Trino étant la surface
d'interrogation, c'est lui qui les définit.

### D21. Le NPL calculé sur le contrat, pas sur les échéances

**Contexte.** Calculé depuis les remboursements observés, le NPL tombait à 0,96 % en
Côte d'Ivoire pour une cible de 5 %. La cause était mesurable : sur trois jours, seuls
**36,9 %** des prêts ont une échéance, un prêt se remboursant mensuellement.

**Décision.** La classification prudentielle est portée par le compte de prêt
(`days_past_due`), comme dans un système bancaire central, et non reconstruite depuis
l'historique des échéances. L'indicateur devient indépendant de la fenêtre de traitement.

**Conséquence assumée.** Une colonne ajoutée au schéma A.2. C'est le prix d'un
indicateur de stock correct : le calculer sur un flux partiel donne un chiffre plausible
mais faux, ce qui est pire qu'une erreur visible.

### D22. L'injection de fraude ne doit pas déplacer les indicateurs

**Contexte.** Gonfler des sinistres à plusieurs fois la prime annuelle, pour alimenter la
règle de fraude du Level 3, faisait passer le loss ratio de 67 % à 126-475 %. La fraude
injectée détruisait l'indicateur qu'elle est censée côtoyer. Le défaut était invisible :
les contrôles portaient sur les données **avant** injection.

**Décision.** Le taux s'applique à la population des sinistres et non à l'ensemble du
lot, le multiple reste juste au-dessus du seuil de la règle, et la charge supplémentaire
est reprise sur les autres règlements **de la même branche**.

**Conséquence assumée.** La compensation par branche n'est pas un détail : la mener
globalement gonflait le ratio de la branche recevant la fraude et affaissait celui des
autres. C'est la maille du KPI qui commande celle de la compensation. Résultat mesuré :
38 couples pays × branche × mois sur 38 dans la fourchette 50-85 %.

### D23. Airflow orchestre, il n'exécute pas

**Contexte.** Les jobs Spark doivent être déclenchés par l'ordonnanceur.

**Décision.** Chaque tâche démarre un conteneur `waba/spark` éphémère via le socket
Docker, plutôt que d'exécuter Spark dans le conteneur Airflow.

**Alternative écartée.** Embarquer Spark et une JVM dans l'image Airflow : près d'un
gigaoctet pour dupliquer un environnement d'exécution existant.

**Conséquence assumée.** Le conteneur Airflow a besoin d'un accès au démon Docker,
accordé par l'ajout du groupe root au seul service concerné — un `chmod` sur le socket
l'ouvrirait à tout l'hôte. Ce découplage est celui que le Level 4 reprendra en
remplaçant `DockerOperator` par `SparkKubernetesOperator`.

### D25. Dépendances déclarées par jeux de données

**Contexte.** Les quatre DAGs s'enchaînent : ingestion, Silver, Gold, reporting.

**Décision.** Le chaînage passe par des `Dataset` Airflow — chaque DAG déclare ce qu'il
produit et ce qu'il consomme — plutôt que par des déclenchements explicites.

**Conséquence assumée.** Ajouter demain un DAG consommant Silver ne demandera aucune
modification en amont, là où un `TriggerDagRunOperator` aurait imposé de modifier le
producteur à chaque nouveau consommateur.

### D24. Trois garde-fous après avoir saturé la machine

**Contexte.** Au premier déploiement, les DAGs actifs par défaut ont déclenché toute la
chaîne, et le rapport réglementaire avec rattrapage automatique depuis une date de départ
vieille de plusieurs mois a mis en file **139 exécutions**, chacune lançant un conteneur
Spark. La mémoire est montée à 7,3 Go sur 9,7.

**Décisions.** Les DAGs sont déployés en pause ; le parallélisme est plafonné à deux
tâches, chaque conteneur Spark réclamant 2 Go ; le rattrapage du rapport réglementaire
est explicite, par `airflow dags backfill` sur la période voulue.

**Conséquence assumée.** Une déclaration réglementaire manquée n'est plus rattrapée
automatiquement. C'est un compromis : la logique voudrait l'inverse, mais un rattrapage
que l'ordonnanceur dimensionne seul est ingérable. En production, la date de départ
serait celle de la mise en service et le rattrapage redeviendrait sans danger.

### D26. Le flux NiFi est construit par script, jamais exporté

**Contexte.** Un flux NiFi se conçoit à la souris. Le réflexe est ensuite d'exporter le
canevas et de verser le fichier au dépôt.

**Décision.** Le flux est construit par appels à l'API REST, dans
[`scripts/nifi_flow.py`](../scripts/nifi_flow.py). Une seule commande le détruit et le
reconstruit à l'identique.

**Alternative écartée.** Le template exporté : plusieurs milliers de lignes de XML ou de
JSON générés, où l'ajout d'un processeur produit un diff illisible et où les identifiants
techniques changent à chaque export. Impossible à relire en revue, impossible à comparer
d'une version à l'autre.

**Conséquence assumée.** Modifier le flux depuis l'interface web est possible mais sans
lendemain : la reconstruction écrase la modification. C'est le prix de la reproductibilité,
et c'est le même contrat que pour l'infrastructure décrite en code. En contrepartie, les
noms de topics ne sont pas recopiés dans le flux : ils sont lus dans `common.domain`, la
même source que les jobs Spark qui les consomment. Une divergence entre producteur et
consommateur ne se serait manifestée que par un topic vide, sans erreur.

### D27. Schéma dérivé de l'en-tête plutôt qu'inféré

**Contexte.** Le lecteur CSV du flux offre l'inférence de schéma : elle devine le type de
chaque colonne, ce qui produit un JSON typé sans écrire un seul schéma. Un fichier
volontairement corrompu a montré ce que cela coûte. `SplitRecord` ne l'a pas routé vers
sa relation d'échec : l'inférence parcourt le fichier entier avant de produire le premier
enregistrement, et l'exception qu'elle lève à ce moment échappe au traitement d'erreur du
processeur. Le lot est annulé, remis en file, rejoué — indéfiniment. Un seul fichier
illisible bloquait toute l'ingestion, et rien n'atteignait jamais la file de rebut.

**Décision.** Le schéma est dérivé de l'en-tête. Le lecteur se construit sans lire le
corps du fichier, l'erreur survient là où le processeur la gère, et le fichier part vers
`dlq-financial-events` avec son motif de rejet en en-tête de message.

**Conséquence assumée.** Tous les champs arrivent en texte dans Kafka ; le typage revient
à la couche Silver. C'est cohérent avec la chaîne batch, où les schémas sont explicites et
jamais devinés — et cela supprime au passage un défaut plus discret : un montant inféré en
entier dans un fichier et en décimal dans le suivant aurait produit deux schémas pour un
même topic.

### D28. Le broker n'est pas une file de rebut

**Contexte.** Un producteur Kafka peut échouer pour deux raisons sans rapport : la donnée
est illisible, ou le broker est indisponible. Les traiter pareillement conduit à déverser
dans la file de rebut des messages parfaitement valides, le jour où Kafka redémarre.

**Décision.** Les deux chemins sont distincts. Un fichier illisible part vers
`dlq-financial-events` avec son contenu et son motif. Un broker injoignable déclenche une
annulation de lot : rien n'est publié, rien n'est perdu, la file d'entrée se remplit.

**Conséquence assumée.** C'est la contre-pression qui prend le relais. Les seuils sont
calibrés en escalier — 500 objets devant le producteur, 200 devant le téléchargement — de
sorte que la saturation remonte jusqu'au recensement, qui cesse de lister. Les fichiers non
traités restent dans MinIO, et le rattrapage est automatique au rétablissement. Le flux
n'absorbe jamais plus que ce que le broker écoule, ce qui est précisément l'inverse du
comportement par défaut (10 000 objets, 1 Go).

### D29. Les messages sont partitionnés par pays

**Contexte.** Kafka ne garantit l'ordre qu'à l'intérieur d'une partition. Sans clé, les
messages sont répartis au hasard et l'ordre des événements d'un même compte est perdu.

**Décision.** La clé du message est le `country_code`, présent dans les quatre jeux de
données.

**Alternative écartée.** L'`account_id`, qui donnerait une garantie plus fine. Mais les
règles du §3.3 — rafales de transactions, seuil AML, couverture de liquidité — s'évaluent
sur des fenêtres par pays, et une clé plus fine disperserait sur toutes les partitions les
événements que ces fenêtres doivent rassembler.

**Conséquence assumée.** Huit pays pour trois partitions : la répartition est inégale, et
la Côte d'Ivoire pèsera plus lourd que le Togo. Sur un broker unique, cela n'a pas de
conséquence ; en production, le nombre de partitions se dimensionnerait sur la volumétrie
du plus gros pays.

### D30. Une seule définition de Silver, deux modes d'exécution

**Contexte.** Le Level 3 demande un job de streaming qui applique « les transformations
Silver » et alimente les tables `silver.*` — les mêmes que le batch du Level 2.

**Décision.** Les constructeurs de `jobs/batch/silver.py` acceptent une source explicite.
Par défaut ils lisent la table brute ; le job de streaming leur passe le micro-lot qu'il
vient de consommer. Les règles de validation sont réutilisées de même : elles s'évaluent
sur des colonnes textuelles, ce que NiFi publie précisément.

**Alternative écartée.** Réécrire les transformations pour le streaming. C'est le chemin
naturel, et le plus dangereux : deux définitions de Silver alimentant les mêmes tables
auraient divergé au premier ajustement de règle métier, et la divergence ne se serait vue
que dans les chiffres — jamais dans une erreur.

**Conséquence assumée.** Le job de streaming dépend du module batch, ce qui peut surprendre
dans une architecture Lambda où l'on présente les deux chemins comme parallèles. C'est
précisément le point : ils sont parallèles en exécution, pas en logique métier.

### D31. Le filigrane porte sur l'heure d'arrivée, pas sur l'heure de l'événement

**Contexte.** L'énoncé demande une déduplication « dans une fenêtre temporelle de 10 min ».
Le réflexe est de poser le filigrane sur l'horodatage métier de la transaction.

**Décision.** Il porte sur l'horodatage d'arrivée dans Kafka.

**Alternative écartée.** L'horodatage métier. Les fichiers rejoués contiennent une journée
entière d'événements de mars 2026 : dès le second micro-lot, le filigrane se serait établi
en fin de journée, et tout événement du matin reçu ensuite aurait été considéré comme
tardif puis écarté. Ce n'est pas une déduplication, c'est une perte silencieuse.

**Conséquence assumée.** La fenêtre protège d'un rejeu rapproché, pas d'un rejeu espacé de
plusieurs heures. C'est le `MERGE` Iceberg qui couvre le second cas : les deux mécanismes
jouent à des échelles de temps différentes, et c'est leur superposition qui rend le job
réellement idempotent — vérifié en rejouant les 4 000 messages, tables inchangées.

### D32. Double récepteur par `foreachBatch`, Iceberg avant Kafka

**Contexte.** Le §3.3 impose d'écrire le résultat dans les topics `silver-*` **et** dans
les tables `silver.*`. Une écriture en continu ne vise qu'un récepteur.

**Décision.** `foreachBatch` reçoit chaque micro-lot comme un DataFrame ordinaire ; le job
fusionne d'abord dans Iceberg, publie ensuite dans Kafka.

**Conséquence assumée.** L'ordre n'est pas indifférent. La table est fusionnée sur la clé
naturelle, donc insensible à un rejeu ; le topic ne l'est pas. En fusionnant d'abord, un
incident entre les deux écritures fait rejouer le micro-lot entier sans dupliquer la table
— seul le topic reçoit un doublon, que le Job 2 dédupliquera sur sa propre fenêtre. Deux
récepteurs sans transaction distribuée ne peuvent pas être atomiques : le choix consiste à
placer l'incohérence possible là où elle se rattrape.

### D33. Les commits de catalogue sont sérialisés

**Contexte.** Quatre flux fusionnent chacun dans sa propre table. Rien ne les oppose
métier — mais au premier essai, trois des quatre ont échoué sur
`CommitStateUnknownException`, l'échec qui laisse ignorer si le commit a été appliqué.

**Diagnostic.** Le catalogue REST persiste ses métadonnées dans un SQLite, qui verrouille
le fichier entier. Le catalogue JDBC valide un commit en ouvrant une transaction en lecture
qu'il promeut en écriture : deux flux simultanés se disputent le verrou, le second reçoit
`SQLITE_BUSY` et le catalogue répond 500.

**Alternative écartée.** Le journal WAL, réflexe habituel face à `SQLITE_BUSY`. Il aggrave
ce cas précis : la promotion d'une transaction de lecture en écriture échoue alors en
`SQLITE_BUSY_SNAPSHOT`, sur lequel aucun délai d'attente n'a de prise. Mesuré, pas supposé.

**Décision.** Un délai d'attente sur le verrou pour les collisions batch/streaming, et un
verrou dans le pilote Spark pour sérialiser les commits des flux entre eux. Il ne coûte
rien : un commit Iceberg est une mise à jour de pointeur, les fichiers Parquet ayant déjà
été écrits en parallèle.

**Conséquence assumée.** C'est une rustine à une limite d'infrastructure, pas une propriété
de l'architecture. Un catalogue adossé à PostgreSQL accepte les écritures concurrentes et
rendrait le verrou inutile ; c'est ce que déploiera le Level 4. Le garder documenté vaut
mieux que le faire disparaître : il dit quelle contrainte pèse sur ce déploiement.

### D34. La prime annuelle appartient au contrat, pas à l'historique des paiements

**Contexte.** La règle de fraude « sinistre supérieur à trois fois la prime annuelle versée »
suppose un montant de référence par police. Le réflexe est de le reconstituer depuis les
échéances observées.

**Mesure.** Sur trois jours de données, 21 % des sinistres réglés seulement portent sur une
police ayant cotisé pendant la fenêtre. La règle serait donc aveugle sur quatre sinistres
sur cinq. Y substituer une prime médiane de branche est pire : les primes s'étalent sur un
facteur dix, et un sinistre normal sur une police chère dépasserait trois fois la médiane
sans rien avoir d'anormal — des faux positifs en série, sur les meilleurs clients.

**Décision.** La prime annuelle rejoint le référentiel des comptes, comme l'avaient fait les
jours d'impayé pour le NPL et pour la même raison : un indicateur prudentiel se calcule sur
le portefeuille entier, pas sur la fraction que la fenêtre d'observation éclaire. Un
assureur connaît la prime de ses contrats ; elle relève des données de référence, pas d'une
inférence.

**Conséquence assumée.** Le schéma de l'annexe A.2 est à nouveau enrichi. Une table Iceberg
déjà créée ne suit pas : `CREATE TABLE IF NOT EXISTS` ne fait rien sur une table existante,
et l'écriture aurait échoué ensuite sur un schéma incompatible, sans indiquer que la cause
est une table restée en arrière. Les jobs comparent désormais le schéma déclaré à celui de
la table et ajoutent les colonnes manquantes. Seuls les ajouts sont automatisés : une
suppression ou un changement de type sont des ruptures de contrat, qui doivent rester des
décisions explicites. Résultat : la règle couvre 100 % des sinistres réglés.

### D35. Une rafale, une alerte

**Contexte.** L'énoncé prescrit une fenêtre de cinq minutes glissant d'une minute. Appliquée
telle quelle, elle produisait 455 alertes là où le générateur avait injecté six rafales.

**Diagnostic.** Deux causes distinctes. La première : une rafale entièrement contenue dans
une fenêtre l'est aussi dans les quatre suivantes, et part donc cinq fois. La seconde, plus
grave : les topics Silver contenaient les doublons de rejeux antérieurs, et une transaction
reçue trois fois suffisait à constituer une fausse rafale à elle seule.

**Décisions.** L'entrée du job est dédupliquée sur la clé naturelle — ce que la conception du
Job 1 annonçait sans que rien ne l'applique. Et les fenêtres décrivant le même épisode sont
regroupées sur le couple compte / première transaction : toutes les fenêtres qui contiennent
la même rafale partagent cette première transaction, ce qui les rassemble exactement, sans
heuristique de recouvrement. Une fenêtre englobant un virement antérieur décrirait un
épisode réellement plus large, et garde son alerte.

**Conséquence assumée.** Le regroupement est aussi une nécessité technique : un `MERGE` dont
la source présente deux fois la même clé échoue. Après correction, le job retrouve
exactement les six comptes de l'oracle, et les dix-huit virements qui les composent.

### D36. L'oracle du générateur sert de test d'acceptation

**Contexte.** Vérifier une détection de fraude en streaming se fait d'ordinaire à l'œil : on
regarde si des alertes sortent. Cela ne dit rien de ce qui n'est pas sorti.

**Décision.** `generator/anomalies.py` expose, en regard de chaque injection, une
implémentation de référence de la règle en pandas. Les alertes du job sont comparées à ce
que cet oracle trouve sur les mêmes fichiers.

**Résultat mesuré.** Concordance exacte sur les quatre règles : 6 comptes en rafale pour 18
virements, 20 paiements d'origine inhabituelle, 1 sinistre excessif, 58 virements au-dessus
du seuil déclaratif. La liste des comptes incriminés est identique des deux côtés.

**Conséquence assumée.** L'oracle et le job partagent leurs seuils, tirés de `common.domain`
— ils ne peuvent donc pas diverger sur un chiffre. Ils ne partagent en revanche aucune ligne
de logique : l'un travaille en pandas sur un fichier, l'autre en Spark sur un flux fenêtré.
C'est cet écart d'implémentation qui donne sa valeur à la concordance.

### D37. Le seuil de liquidité est un paramètre, la règle ne l'est pas

**Contexte.** L'énoncé demande une alerte « si un seuil de couverture minimal est franchi »,
sans le chiffrer. La règle retenue compare les sorties nettes d'un pays sur la fenêtre à ses
encours.

**Constat.** Sur un jeu de données échantillonné — quelques centaines de transactions par
pays et par jour face aux encours de 250 000 comptes — le ratio observé reste inférieur au
millionième. Aucun seuil réglementaire crédible ne se déclenche.

**Décision.** La valeur réglementaire reste dans `common.domain` et le job la lit ; elle est
surchargeable par variable d'environnement, et **chaque alerte enregistre le seuil qui l'a
produite**. Le mécanisme se démontre donc sans travestir la norme, et une alerte issue d'un
seuil de démonstration reste identifiable comme telle.

**Alternative écartée.** Fixer par défaut un seuil assez bas pour que la démonstration
produise des alertes. C'eût été présenter comme réglementaire une valeur choisie pour faire
joli sur une capture d'écran.

### D38. La requête Lambda doit d'abord ne pas compter deux fois

**Contexte.** L'énoncé illustre le §3.4 par une jointure externe entre la table Gold
consolidée et le topic Silver, dont les montants sont additionnés. Écrite telle quelle,
elle donne un résultat faux.

**Diagnostic.** Le Job 1 écrit chaque événement dans la table Silver **et** dans le topic
Silver, et la table Gold est calculée depuis la première. Les deux sources décrivent donc
les mêmes transactions : les additionner compte deux fois tout ce que le batch a déjà
consolidé. Le problème n'est pas la jointure, c'est le recouvrement.

**Décision.** La borne de partage est l'horodatage du dernier calcul Gold. Le batch fait foi
jusqu'à cette borne ; seuls les événements traités par le streaming après elle viennent s'y
ajouter. C'est la « vue temps réel » du modèle Lambda, et elle se lit dans une colonne de la
requête : chaque ligne indique si elle vient du batch, du flux, ou des deux.

**Conséquence assumée.** La requête dépend d'un horodatage de traitement présent des deux
côtés — `processed_at`, apposé par le Job 1 sur chaque ligne Silver et par le job Gold sur
chaque agrégat. C'est une colonne technique, et c'est elle qui rend la fusion exacte plutôt
qu'approximative. Vérifié : après dépôt de deux nouveaux fichiers, la requête isole
correctement 400 opérations en temps réel pour le Burkina et le Togo, et laisse les six
autres pays en « batch seul ».

### D39. Le fuseau de session de Trino est forcé

**Contexte.** Trino tire son fuseau de session de la JVM. Un horodatage Iceberg est stocké
avec fuseau, un horodatage décodé depuis un message Kafka ne l'est pas.

**Décision.** Le conteneur Trino tourne en UTC, comme les sessions Spark.

**Conséquence assumée.** Sans cela, la borne de partage de la requête Lambda comparerait des
instants décalés du fuseau de la machine de développement, et le partage batch/streaming
serait faux d'une heure ou deux — sans la moindre erreur, juste des chiffres légèrement
différents. Huit pays sur trois fuseaux : la question n'est pas théorique.

### D40. Batch et streaming lisent la même zone : un délai de grâce, pas un interrupteur

**Contexte.** Le §1.2 exige d'archiver les fichiers traités ; le §3.1 fait surveiller la même
zone d'atterrissage par NiFi. Les deux chemins se disputent donc les mêmes objets, et
l'énoncé ne dit rien de leur cohabitation.

**Le conflit, précisément.** Le job batch archive dès l'ingestion, toutes les quinze minutes ;
NiFi recense toutes les trente secondes. Un fichier archivé avant d'avoir été recensé
disparaît du chemin streaming **sans laisser de trace** — aucune erreur, juste des événements
qui n'arrivent jamais. Archivé entre le recensement et le téléchargement, il produit à
l'inverse un rejet qui n'en est pas un, et pollue la file de rebut.

**Alternative écartée.** Désactiver l'archivage au Level 3. C'était la solution la plus
simple, et elle revenait à abandonner silencieusement une exigence du Level 1 dès qu'on
active le niveau suivant — la zone d'atterrissage ne se viderait plus jamais.

**Décision.** L'archivage est conservé, assorti d'un délai de grâce : un fichier n'est
archivé qu'après dix minutes de présence. Le coût est nul aux niveaux inférieurs — les
fichiers partent simplement à la passe suivante — et le rejeu de l'ingestion entre-temps est
sans effet grâce au `MERGE`.

**Conséquence assumée.** Le délai doit rester supérieur à la période de recensement de NiFi,
et l'un ne connaît pas l'autre. C'est une coordination implicite, que le Level 4 rendra
explicite : NiFi acquittera ce qu'il a lu, et l'archivage n'attendra plus un délai mais un
accusé de réception. Le paramètre reste surchargeable, et le test de fumée vérifie qu'un
fichier tout juste déposé survit à un passage du batch.

### D41. Le filigrane doit couvrir l'étalement des données, pas le retard attendu

**Contexte.** Les règles à fenêtre du Job 2 portent sur l'horodatage métier : une rafale se
définit par cinq minutes vécues par le compte, pas par cinq minutes de traitement. Le
filigrane a d'abord été dimensionné comme on le fait d'ordinaire — la tolérance au retard
d'un événement, douze heures.

**Mesure.** Le test de fumée a rejoué l'intégralité des topics et trouvé **deux rafales de
plus** qu'à l'exécution précédente. Ce n'étaient pas des doublons — aucune alerte n'apparaît
deux fois pour un même compte : la détection elle-même n'était pas déterministe.

**Diagnostic.** Les fichiers rejoués couvrent trois jours. Un fichier daté du 29 mars arrivant
après un fichier du 31 voit tous ses événements écartés comme tardifs, et le résultat dépend
alors du découpage des micro-lots — invisible, puisque rien n'échoue.

**Décision.** Le filigrane couvre l'étalement temporel complet du jeu rejoué, trois jours et
non douze heures. L'état reste borné par cette durée, et il est négligeable ici — quelques
milliers de clés.

**Conséquence assumée.** C'est un paramètre de déploiement, pas une constante : en
production, où un événement parvient au job quelques secondes après la transaction, quelques
minutes suffiraient et l'état s'en trouverait d'autant plus léger. Ce qui compte est la
règle de dimensionnement, que ce projet a apprise en la ratant : le filigrane se dimensionne
sur le désordre réel de la source, pas sur le retard qu'on imagine.

### D42. Le mode journal de SQLite vit dans le fichier, pas dans la connexion

**Contexte.** Après la mise en service des DAGs, les quatre ont échoué. La trace remontait à
`ServiceFailureException: 500` du catalogue, et sous elle, `SQLITE_BUSY_SNAPSHOT`.

**Ce qui rend le diagnostic intéressant.** Ce code d'erreur est **propre au journal WAL** — or
la configuration ne l'activait plus depuis plusieurs jours : le paramètre avait été retiré de
la chaîne de connexion après l'avoir mesuré nuisible (cf. D33). Il était pourtant toujours
actif.

`PRAGMA journal_mode=WAL` inscrit le mode **dans l'en-tête du fichier de base**, où il
survit à toute reconnexion. Retirer le paramètre de la configuration ne le désactive pas : il
n'empêche que de le réactiver. La correction précédente n'avait donc jamais pris effet, et
rien ne le signalait — les tests passaient parce que le verrou du pilote Spark masquait le
problème tant que les écritures venaient d'un seul processus.

**Décision.** Rétablissement explicite du journal classique, service arrêté :
`PRAGMA wal_checkpoint(TRUNCATE)` puis `PRAGMA journal_mode=DELETE`. Le point de contrôle
n'est pas facultatif — le fichier principal ne pesait que 32 Ko quand le WAL en contenait
778, soit l'essentiel des métadonnées des 34 tables.

**Et la cause de fond.** Les tâches Airflow s'exécutent chacune dans son propre conteneur :
le verrou du pilote Spark, qui protégeait les flux d'un même processus, n'a aucune prise sur
elles. Un **pool Airflow à un emplacement** exprime la contrainte là où elle vaut pour tout
l'ordonnanceur. Les tâches Spark s'exécutent donc en série — ce qu'elles faisaient déjà de
fait, chacune réclamant 2 Go sur une machine de 16.

**Conséquence assumée.** Deux mécanismes de sérialisation coexistent, chacun à sa portée, et
un catalogue adossé à PostgreSQL les rendrait tous deux inutiles. C'est la limite de fond :
SQLite a été choisi pour sa légèreté, et ce choix se paie en concurrence. Le Level 4 le
remplacera ; d'ici là, la contrainte est explicite et documentée plutôt que subie.
