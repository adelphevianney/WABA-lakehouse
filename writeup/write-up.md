# WABA Group — Plateforme analytique multi-pays

**Write-up technique** — choix d'architecture, compromis et limites connues.
Le journal détaillé des cinquante décisions se trouve en annexe
([`decisions.md`](decisions.md)) ; ce document en tire la synthèse.

---

## 1. Ce qui a été construit

| Niveau | Périmètre | État |
|---|---|---|
| **1** | Générateur, MinIO, Spark, Iceberg, Trino | Complet — 8 tables `raw.*` idempotentes, 9 contrôles de fumée |
| **2** | Médaillon Bronze/Silver/Gold, Airflow | Complet — 7 KPIs, 4 DAGs chaînés par jeux de données, 9 contrôles |
| **3** | NiFi → Kafka, 2 jobs Structured Streaming, requête Lambda | Complet — DLQ, double récepteur, 10 contrôles |
| **4** | Kubernetes, Superset, observabilité | Partiel — 13 pods déployés, 3 tableaux de bord, 3 alertes. **SSO Keycloak et OpenMetadata non traités.** |

134 fichiers versionnés, 230 tests unitaires, trois tests de fumée exécutables
d'une commande. La plateforme redémarre intégralement depuis un dépôt vierge :
`make up-l4` puis `make smoke-l1`.

---

## 2. L'architecture

```
Streamlit ──▶ raw-landing ──┬──▶ Spark batch ──▶ Iceberg (raw → silver → gold) ──┐
   (générateur)             │         ▲                                          │
                            │      Airflow                                       ├──▶ Trino ──▶ Superset
                            │                                                    │      ▲
                            └──▶ NiFi ──▶ Kafka ──▶ Spark Streaming ─────────────┘      │
                                            └─────── connecteur Kafka ──────────────────┘
```

Un **pattern Lambda** autour d'un stockage objet unique. Les deux chemins lisent
la même zone d'atterrissage et écrivent dans les mêmes tables Iceberg ; Trino les
expose ensemble, le lakehouse par son catalogue, le bus par son connecteur Kafka.

Trois choix de socle méritent d'être justifiés.

**Catalogue Iceberg REST plutôt que Hive Metastore.** Le sujet autorise les deux.
Le Metastore aurait imposé un PostgreSQL dédié et environ 1,5 Go de mémoire pour
un service dont ce projet n'utilise que la fonction d'annuaire. Ce choix se paie
en concurrence, et c'est développé plus bas.

**Iceberg plutôt que Delta ou Hudi.** Le `MERGE INTO` sur la clé naturelle est ce
qui rend chaque job rejouable, et le partitionnement caché — `days(timestamp)` —
évite d'inscrire la granularité temporelle dans les requêtes. Une réingestion
complète de la couche brute n'ajoute pas une ligne, ce que le test de fumée
vérifie à chaque exécution.

**Spark en mode local.** Aucun cluster n'est déployé : les jobs s'exécutent dans
un conteneur éphémère de 2 Go, démarré par Airflow. C'est la contrainte matérielle
qui l'impose, et c'est la limite la plus structurante du projet — §5.

---

## 3. Cinq décisions structurantes

### 3.1 Inverser la calibration : fixer la cible, puis générer

L'énoncé exige que le NPL tombe entre 3 % et 8 % et le ratio sinistres/primes
entre 50 % et 85 %. Une génération aléatoire ne peut pas le garantir : le taux de
défaut résulterait des poids de tirage, et le loss ratio du rapport fortuit entre
deux lois indépendantes — typiquement plusieurs centaines de pour cent.

Le générateur inverse donc la logique. Chaque pays reçoit une cible stable,
dérivée par hachage de son code ; les comptes en défaut, les jours d'impayé et la
charge sinistres sont ensuite dimensionnés pour l'atteindre. Toutes les décisions
sont **déterministes et sans état** : rejouer la génération produit exactement les
mêmes comptes en défaut, et les fichiers peuvent être produits pays par pays et
jour par jour dans n'importe quel ordre.

Résultat mesuré : NPL conforme sur 8 pays sur 8, loss ratio sur 76 couples
pays × branche sur 76.

Cette inversion a eu une conséquence imprévue. Injecter des fraudes à l'assurance
— des sinistres à plusieurs fois la prime annuelle — faisait passer le loss ratio
de 67 % à 475 % : la fraude détruisait l'indicateur qu'elle était censée côtoyer.
La charge supplémentaire est désormais reprise sur les autres règlements **de la
même branche**, parce que c'est la maille du KPI qui commande celle de la
compensation.

### 3.2 Une seule définition de Silver, deux modes d'exécution

Le Level 3 demande un job de streaming qui applique « les transformations
Silver » — les mêmes que celles du batch, sur les mêmes tables.

Les constructeurs de `jobs/batch/silver.py` acceptent une source explicite. Par
défaut ils lisent la table brute ; le job de streaming leur passe le micro-lot
qu'il vient de consommer dans Kafka. Les règles de validation suivent le même
chemin : les huit règles de `quality.py` s'évaluent sur des colonnes encore
textuelles, ce que NiFi publie précisément.

L'alternative — réécrire les transformations pour le streaming — était le chemin
naturel et le plus dangereux : deux définitions de Silver alimentant les mêmes
tables auraient divergé au premier ajustement de règle métier, et la divergence
ne se serait vue que dans les chiffres, jamais dans une erreur.

### 3.3 Tout ce qui se clique se scripte

Le flux NiFi et les tableaux de bord Superset sont construits par appels d'API,
non exportés d'une session manuelle. Un export est une archive de plusieurs
milliers de lignes générées, où l'ajout d'un composant produit un diff illisible
et où les identifiants changent à chaque fois. `scripts/nifi_flow.py` et
`scripts/superset_dashboards.py` tiennent chacun dans un fichier relisible, se
rejouent à l'identique sur une installation vierge, et tirent leurs constantes
des mêmes modules que le reste — les noms de topics de `common.domain`, les seuils
réglementaires du domaine.

Le bénéfice s'est vérifié en incident : après avoir dû remettre à neuf la
configuration de NiFi pour régénérer son certificat, le flux — neuf processeurs,
six services de contrôle, dix connexions — a été reconstruit par une commande.

### 3.4 L'oracle du générateur comme test d'acceptation

Vérifier une détection de fraude en streaming se fait d'ordinaire à l'œil : on
regarde si des alertes sortent. Cela ne dit rien de ce qui n'est pas sorti.

`generator/anomalies.py` expose, en regard de chaque injection, une implémentation
de référence de la règle en pandas. Les alertes du job Spark sont comparées à ce
que cet oracle trouve sur les mêmes fichiers. Les deux partagent leurs seuils,
tirés de `common.domain` — ils ne peuvent donc pas diverger sur un chiffre — mais
aucune ligne de logique : l'un travaille en pandas sur un fichier, l'autre en
Spark sur un flux fenêtré. C'est cet écart d'implémentation qui donne sa valeur à
la concordance.

| Règle | Oracle | Job 2 |
|---|---|---|
| Rafales de virements | 6 comptes, 18 virements | **6 comptes, 18 virements** |
| Origine inhabituelle | 20 paiements | **20 paiements** |
| Sinistre > 3× la prime | 1 sinistre | **1 sinistre** |
| Seuil déclaratif BCEAO | 58 virements | **58 virements** |

La liste des comptes incriminés est identique des deux côtés.

### 3.5 L'idempotence, en couches superposées

Le rejeu est la propriété la plus vérifiée du projet, et aucun mécanisme ne suffit
seul.

Le `MERGE INTO` sur la clé naturelle rend chaque écriture Iceberg rejouable ; le
prédicat inclut la colonne de partition, sans quoi chaque fusion balaierait la
table entière. En streaming, une déduplication sur fenêtre glissante écarte en
amont les doublons rapprochés, sans état non borné. Les deux jouent à des échelles
de temps différentes, et c'est leur superposition qui tient : les 4 000 messages
des topics rejoués depuis le début laissent les tables Silver strictement
inchangées.

Un piège s'y cache. Le filigrane du Job 1 porte sur l'heure d'arrivée dans Kafka,
pas sur l'heure de l'événement : les fichiers rejoués contiennent une journée
entière d'opérations passées, et un filigrane métier aurait écarté comme tardif
tout événement du matin reçu après ceux du soir. Ce n'est pas une déduplication,
c'est une perte silencieuse. Le Job 2, dont les règles portent par nature sur le
temps de l'événement, doit au contraire l'utiliser — avec un filigrane
dimensionné sur l'étalement complet du jeu rejoué, faute de quoi la détection
cesse d'être déterministe. Mesuré : douze heures de filigrane sur trois jours de
données faisaient varier le nombre de rafales détectées d'une exécution à l'autre.

---

## 4. Ce que seize gigaoctets ont imposé

La plateforme complète du Level 4 dépasse 24 Go. Le développement s'est fait sur
une machine de 16 Go, dont 9,7 alloués à Docker. Trois conséquences.

**Des profils exclusifs plutôt que cumulatifs.** `l1`, `l2`, `l3`, `l4` ne
s'empilent pas : chacun démarre ce que son niveau exige, et chaque service porte
une limite mémoire explicite. Le Level 3 complet tient en 3,4 Go.

**Des écritures de catalogue sérialisées.** SQLite n'accepte qu'un écrivain à la
fois. Un verrou dans le pilote Spark protège les flux d'un même processus ; un
pool Airflow à un seul emplacement couvre les tâches batch, qui tournent chacune
dans son propre conteneur. Deux mécanismes pour une même contrainte, parce qu'ils
n'ont pas la même portée — et un catalogue adossé à PostgreSQL les rendrait tous
deux inutiles.

**Un médaillon recalculé sur une fenêtre.** Le jeu complet de l'annexe couvre
90 jours et 22 millions de lignes en couche brute. Une reconstruction intégrale
de Silver dépasse ce qu'un pilote de 2 Go absorbe. Chaque exécution planifiée est
donc bornée aux sept derniers jours de données présents dans la source ; la couche
brute, elle, conserve tout. C'est de toute façon le régime nominal d'un médaillon
en exploitation — un recalcul complet est une opération exceptionnelle.

---

## 5. Limites connues

**Le volume complet n'est pas traitable en une passe.** Reconstruire Silver sur
les 22 millions de lignes échoue en `OutOfMemoryError`. Ce n'est pas un défaut de
la chaîne mais de son mode d'exécution : Spark en local sur un poste partagé avec
le reste de la pile. Un cluster — ce que vise le déploiement Kubernetes — lève la
contrainte sans changer une ligne de code métier.

**Le SSO Keycloak et le catalogue OpenMetadata ne sont pas traités.** Le namespace
`governance` et la base qui les accueillerait existent ; rien d'autre. C'est
l'arbitrage assumé du dernier jour : livrer un write-up et une démonstration
plutôt qu'un cinquième composant à moitié intégré.

**Le Spark Operator n'est pas déployé.** C'est un opérateur tiers, installé par son
propre chart. Les droits que l'ordonnanceur utilisera pour lui soumettre des
`SparkApplication` — compte de service, Role, RoleBinding — sont en revanche
déclarés, de sorte que son ajout ne demande aucune reprise du reste.

**Deux alertes sur trois mesurent autre chose que ce que l'énoncé nomme.** Le
décalage par groupe de consommateurs n'existe pas : Spark gère ses offsets dans
son point de reprise et ne les valide pas auprès du broker. L'alerte mesure donc
l'écart entre topics bruts et topics Silver, vu depuis le broker. Chaque écart est
écrit dans la description de la règle, là où l'exploitant le lira au moment où elle
sonne.

**Un seul broker Kafka, un seul coordinateur Trino.** Aucune réplication, aucune
haute disponibilité. Les manifestes Kubernetes en portent la trace — facteur de
réplication à 1, stratégie `Recreate` sur les services à état.

**L'énoncé contient deux erreurs factuelles**, suivies telles quelles parce que la
grille en dépend : la Guinée-Conakry n'est pas membre de l'UEMOA et n'utilise pas
le franc CFA, et la CIMA ne régule pas le Ghana, qui relève de sa propre autorité.

---

## 6. Ce que je referais autrement

**PostgreSQL comme catalogue dès le premier jour.** SQLite a été choisi pour sa
légèreté et a coûté deux incidents : des commits concurrents en échec, puis un
mode journal WAL inscrit dans le fichier — donc survivant à toute reconfiguration
— qui a mis les quatre DAGs à terre. Les 300 Mo économisés ne valaient pas ces
heures.

**Dimensionner les réglages Spark sur le volume cible, pas sur l'échantillon.**
`shuffle.partitions` fixé à 8 était juste sur seize mille lignes et faux sur trois
millions. Le bon réglage n'était pas un autre nombre mais l'exécution adaptive,
qui regroupe après coup en visant une taille.

**Tester les alertes dans les deux sens.** Une alerte qui se déclenche prouve peu ;
c'est en la faisant se taire qu'on découvre les biais de sa métrique. C'est ainsi
qu'a été trouvé l'écart structurel qui aurait fait sonner l'alerte de retard en
permanence.

---

*Annexe : [`decisions.md`](decisions.md) — cinquante décisions, chacune avec son
contexte, l'alternative écartée et la conséquence assumée.*
