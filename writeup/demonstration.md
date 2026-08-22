# Démonstration — plan de tournage

Vidéo de 5 à 10 minutes. Ce plan vise **8 min 30**, ce qui laisse de la marge
sans frôler la borne basse.

L'énoncé impose cinq séquences : génération multi-pays, exécution d'un pipeline,
requête Trino, alerte de fraude déclenchée, tableau de bord Superset. Elles sont
toutes ici, plus le déploiement Kubernetes.

---

## Avant d'enregistrer

**La séquence Kubernetes se tourne en premier**, cluster allumé et pile Compose
éteinte : les deux ne tiennent pas simultanément dans les neuf gigaoctets
alloués à Docker. On bascule ensuite pour tout le reste.

```bash
make k8s-up && make k8s-status     # séquence 7, à filmer d'abord
make k8s-down                       # puis libérer
make up-l4                          # la pile Compose, pour les séquences 1 à 6
make demo-reset                     # données fraîches, topics propres, flux NiFi armé
```

`make demo-reset` remet la plateforme dans un état connu : il purge les topics,
efface les points de reprise, reconstruit le flux NiFi et dépose un jeu de
données de démonstration sur les huit pays. Compter cinq minutes.

Trois onglets de navigateur ouverts à l'avance, connectés :
Superset (8088), Grafana (3000), Airflow (8090). NiFi (8091) accepte l'exception
de certificat une fois pour toutes.

Un terminal en police large, dans le dépôt.

### Ce que la répétition a donné

Le plan a été joué en entier avant d'être écrit. Avec le jeu que `demo-reset`
dépose — huit pays, 600 lignes par fichier, soit 16 800 événements — les
séquences produisent :

| Ce qui s'affiche | Valeur observée |
|---|---|
| Offsets sur les topics bruts après NiFi | 16 829 |
| Job 1 | 2 400 à 4 800 validés par jeu de données, 0 rejeté |
| Rafales de virements | 28 alertes, 84 virements, sur les 8 pays |
| Origine inhabituelle | 48 alertes, sur les 4 pays du mobile money |
| Sinistre excessif | 7 alertes, sur 7 pays |
| Événements AML | 403 |

**Les deux jobs de streaming prennent trois à quatre minutes chacun** à ce
volume. Ne pas filmer l'attente : lancer, commenter pendant, revenir au
résultat — ou couper au montage. Pour une exécution plus rapide en direct,
`--rows 200` suffit à faire sortir les trois règles, avec des compteurs plus
modestes.

---

## Séquence 1 — Le problème et la forme de la réponse · 0:00 → 0:50

**Montrer** le `README.md`, puis `make help`.

> WestAfrica BancAssur, huit pays, quatre entités — banque, assurance, mobile
> money, microfinance. Il fallait une plateforme analytique qui traite ces flux
> en batch et en temps réel, et qui produise des indicateurs réglementaires
> BCEAO et CIMA.
>
> Tout se pilote par une seule entrée : `make` sous Linux et macOS, `waba.ps1`
> sous Windows. Quatre niveaux, quatre profils Docker exclusifs — la machine de
> développement fait seize gigaoctets, ils ne s'empilent pas.

**Ne pas** lire l'arborescence à voix haute. Elle se voit.

---

## Séquence 2 — Génération multi-pays · 0:50 → 2:00

**Montrer** l'interface Streamlit sur http://localhost:8501, puis la console
MinIO sur http://localhost:9001, bucket `raw-landing`.

```bash
docker compose --env-file .env -f docker/compose.yml exec streamlit \
  python -m generator.seed --reuse-referentials --countries CI SN BF --days 1 --rows 800
```

> Le générateur ne tire pas au hasard. L'énoncé exige que le taux de créances
> douteuses tombe entre 3 et 8 %, et le ratio sinistres/primes entre 50 et 85 %.
> Une génération aléatoire ne peut pas le garantir. Alors on inverse : on fixe
> d'abord la cible de chaque pays, puis on génère les données qui l'atteignent.
>
> Les fichiers arrivent dans la zone d'atterrissage, rangés par pays et par type,
> à la nomenclature de l'annexe.

**Insister** sur l'inversion de la calibration. C'est le point le plus
distinctif du projet et il ne se voit pas dans le code sans explication.

---

## Séquence 3 — Le pipeline batch · 2:00 → 3:20

**Montrer** Airflow sur http://localhost:8090, onglet **Datasets**.

```bash
make ingest-l1
```

> Quatre DAGs, mais aucun ne nomme son voisin. Chacun déclare ce qu'il produit
> et ce qu'il consomme, et Airflow enchaîne. Ajouter demain un consommateur de
> Silver ne demandera aucune modification en amont.

**Montrer** le graphe des jeux de données, puis un run de `dag_bronze_to_silver`
déclenché par `dag_ingest_raw`.

> Le point qui compte : le job d'ingestion est idempotent. Le même fichier
> réingéré n'ajoute pas une ligne — c'est un `MERGE` sur la clé naturelle, et le
> test de fumée le vérifie à chaque exécution.

---

## Séquence 4 — Requêtes Trino et KPIs réglementaires · 3:20 → 4:30

```bash
make queries-l2
```

**Laisser défiler** la sortie, s'arrêter sur les KPIs réglementaires.

> Le NPL par pays, le ratio sinistres/primes par branche. Les seuils affichés —
> 5 % pour la BCEAO, 70 % pour la CIMA — ne sont pas recopiés dans la requête :
> ils viennent du module de domaine, celui-là même qu'appliquent les jobs.
>
> Huit pays sur huit dans la fourchette. Soixante-seize couples pays-branche sur
> soixante-seize.

---

## Séquence 5 — Le chemin temps réel · 4:30 → 6:15

**Montrer** le canevas NiFi sur https://localhost:8091/nifi.

> Neuf processeurs, construits par script et non dessinés à la souris. Un export
> NiFi fait plusieurs milliers de lignes générées ; ce flux tient dans un fichier
> qu'on relit. Il recense le bucket, écarte les référentiels, découpe chaque CSV
> en événements JSON et publie dans le topic de son jeu de données.

```bash
make topics
```

**Déposer** un fichier depuis Streamlit, **montrer** les offsets qui montent.

```bash
make stream-silver-once
make stream-gold-once
```

> Le premier job valide chaque message et écrit dans deux destinations : le topic
> Silver et la table Iceberg. Le second applique les trois règles de fraude et la
> surveillance AML.
>
> Et voilà le point que je tiens à montrer.

```bash
docker compose --env-file .env -f docker/compose.yml exec -T trino trino \
  --output-format ALIGNED --execute \
  "SELECT alert_type, count(*) alertes, sum(occurrences) evenements FROM iceberg.gold.fraud_alerts GROUP BY 1 ORDER BY 1"
```

> Le générateur expose, en regard de chaque injection de fraude, une
> implémentation de référence de la règle en pandas. Les alertes du job Spark
> sont comparées à ce que cet oracle trouve sur les mêmes fichiers. Les deux
> partagent leurs seuils mais aucune ligne de logique — l'un travaille en pandas
> sur un fichier, l'autre en Spark sur un flux fenêtré.
>
> Concordance exacte sur les quatre règles, et la même liste de comptes
> incriminés des deux côtés.

**C'est la séquence à ne pas bâcler.** L'alerte de fraude est un critère explicite
de la grille, et l'oracle est ce qui la rend crédible.

---

## Séquence 6 — La requête Lambda, puis Superset · 6:15 → 7:45

```bash
docker compose --env-file .env -f docker/compose.yml exec -T trino trino \
  --output-format ALIGNED -f /sql/level3/01_lambda_unifiee.sql
```

**Montrer** la colonne `provenance`.

> Trino lit le lakehouse par son catalogue Iceberg et le bus par son connecteur
> Kafka. Le piège n'est pas la jointure, c'est le recouvrement : les deux sources
> décrivent les mêmes événements, et les additionner compterait deux fois ce que
> le batch a déjà consolidé. La borne de partage est l'horodatage du dernier
> calcul Gold — chaque ligne dit d'où elle vient.

**Basculer** sur Superset, http://localhost:8088. Ouvrir les trois tableaux de
bord, s'arrêter sur *Risque & Conformité* et son formatage conditionnel.

> Trois tableaux, onze graphiques, construits par appels d'API comme le flux
> NiFi. Le filtre par pays s'applique à tous les graphiques d'un tableau.

---

## Séquence 7 — Observabilité et Kubernetes · 7:45 → 8:30

**Montrer** Grafana, http://localhost:3000, tableau *Santé des pipelines*, puis
la page des alertes.

> Cinq mécanismes de collecte pour cinq composants, parce qu'aucun n'expose ses
> métriques de la même façon. NiFi a même demandé d'écrire un adaptateur : sa
> version 2 protège ses métriques par un jeton qui expire, là où Prometheus ne
> sait présenter qu'un jeton fixe.
>
> Trois alertes. Celle-ci, je l'ai déclenchée pour de bon en laissant trente
> mille messages s'accumuler, puis je l'ai fait retomber — et c'est en la faisant
> retomber que j'ai découvert un biais dans ma métrique.

**Enchaîner** sur la capture Kubernetes tournée en début de session.

> Et toute la plateforme se déploie sur Kubernetes en une commande : cinq
> namespaces par domaine, sondes, volumes persistants, secrets hors du dépôt.
> Treize pods.

---

## Clôture · 8:30

> Ce qui n'est pas fait : le SSO Keycloak et le catalogue OpenMetadata. C'est un
> arbitrage du dernier jour — livrer une démonstration et un write-up plutôt
> qu'un cinquième composant à moitié intégré. Les limites connues sont dans le
> write-up, avec ce que je referais autrement.

---

## Ce qu'il ne faut pas faire

**Ne pas lire le code à l'écran.** Montrer ce qui tourne, expliquer pourquoi.

**Ne pas attendre en silence** qu'un job Spark se termine — la reconstruction
Silver prend quatre minutes au volume complet. Lancer, parler pendant, revenir au
résultat. Ou couper au montage.

**Ne pas promettre ce qui n'est pas là.** Le SSO et le catalogue sont annoncés
comme absents, pas contournés.

**Ne pas s'excuser** des limites. Elles sont mesurées et documentées ; c'est une
force, pas un aveu.
