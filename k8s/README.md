# Déploiement Kubernetes — Level 4.1

Toute la plateforme se déploie en une commande sur un cluster local (Docker
Desktop, Minikube ou kind) :

```bash
cp k8s/base/secrets.env.example k8s/base/secrets.env   # puis y mettre de vraies valeurs
make k8s-up
```

`make k8s-up` rend les manifestes et les applique. Le rendu seul, pour relire ce
qui va être créé :

```bash
make k8s-render        # écrit le manifeste complet sur la sortie standard
make k8s-status        # état des pods, par namespace
make k8s-down          # supprime tout, volumes compris
```

## Structure

```
k8s/
├── base/
│   ├── namespaces.yaml          # un namespace par domaine (§4.1)
│   ├── ingress.yaml             # les cinq interfaces, routées par nom d'hôte
│   ├── secrets.env.example      # liste des secrets attendus
│   ├── serving/                 # MinIO, catalogue Iceberg, Trino
│   ├── ingestion/               # Kafka, NiFi
│   ├── processing/              # Airflow et sa base
│   ├── governance/              # Superset et la base de gouvernance
│   └── monitoring/              # Prometheus, Grafana, Loki
├── components/
│   └── config-commune/          # ConfigMap et Secret, partagés par les domaines
└── overlays/
    └── local/                   # ressources réduites pour un poste de travail
```

**Kustomize plutôt que Helm.** Le sujet accepte les deux. Kustomize est intégré à
kubectl — rien à installer — et les manifestes restent du Kubernetes ordinaire,
relisibles sans connaître un langage de gabarit.

**Un composant pour la configuration commune.** Un Secret et une ConfigMap sont
des objets *par namespace* : les générer une fois et les partager entre les cinq
domaines est impossible. Le composant évite de recopier leur déclaration cinq
fois.

**Le suffixe de hachage est conservé.** Modifier une valeur crée un nouvel objet
et redéploie les pods qui le consomment. Sans lui, un changement de configuration
resterait sans effet jusqu'au prochain redémarrage — panne silencieuse classique.

## Images construites par le dépôt

`waba/airflow:dev`, `waba/superset:dev` et `waba/spark:dev` ne viennent d'aucun
registre public. Sur un cluster partageant le démon de l'hôte — Docker Desktop —
`imagePullPolicy: IfNotPresent` suffit après `make up-l4`. Ailleurs, il faut les
pousser dans un registre joignable, ou les charger dans les nœuds :

```bash
docker save waba/superset:dev | docker exec -i <noeud> ctr -n k8s.io images import -
```

## Accès aux interfaces

Les Ingress routent par nom d'hôte. Sans contrôleur Ingress installé, le plus
simple reste la redirection de port :

```bash
kubectl port-forward -n waba-serving svc/trino 8080:8080
kubectl port-forward -n waba-governance svc/superset 8088:8088
kubectl port-forward -n waba-monitoring svc/grafana 3000:3000
```

Avec un contrôleur, ajouter au fichier `hosts` de la machine :

```
127.0.0.1 trino.waba.local superset.waba.local grafana.waba.local airflow.waba.local nifi.waba.local minio.waba.local
```

## Quatre pièges rencontrés, et leur correction

Ces manifestes ont été déployés pour de bon, pas seulement rendus. Les quatre
points suivants ne se voient qu'à l'exécution.

**Kubernetes écrase les variables d'environnement des applications.** Pour chaque
Service d'un namespace, il injecte des variables héritées de Docker : le Service
`superset` produit `SUPERSET_PORT=tcp://10.x.x.x:8088`. Superset lit une variable
du même nom, y attend un numéro de port, et refuse de démarrer sur *'tcp' is not
a valid port number*. `enableServiceLinks: false` coupe l'injection — renommer le
Service ne ferait que repousser la collision.

**Un broker KRaft ne peut pas devenir prêt.** Il s'enregistre auprès de son
contrôleur — lui-même — via son nom DNS, or un Service sans tête ne publie
l'adresse d'un pod qu'une fois celui-ci prêt. L'œuf et la poule.
`publishNotReadyAddresses: true` rompt le cycle.

**Une sonde peut coûter plus cher que ce qu'elle surveille.**
`kafka-broker-api-versions.sh` lance une JVM à chaque appel : toutes les dix
secondes, sur un nœud contraint, elle provoquait les délais qu'elle était censée
détecter. La sonde par socket est moins précise et laisse le broker respirer.

**Une configuration absente ne dit pas son nom.** Sans
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, Airflow se rabat silencieusement sur
SQLite, que le LocalExecutor refuse — et le message d'erreur parle d'exécuteur,
pas de configuration manquante.

## Ce que ces manifestes ne couvrent pas

Le **Spark Operator** n'est pas déployé ici : c'est un opérateur tiers, installé
par son propre chart. Les droits que l'ordonnanceur Airflow utilisera pour lui
soumettre des `SparkApplication` sont en revanche déclarés — compte de service,
Role et RoleBinding — de sorte que son ajout ne demande aucune reprise du reste.

**Keycloak et OpenMetadata** ne sont pas déployés. Le namespace `governance` et
la base qui les accueillera existent.
