# Image d'exécution des jobs PySpark de la plateforme WABA.
#
# Deux familles de jars sont nécessaires, et il est facile de croire qu'une
# seule suffit :
#
#   * iceberg-spark-runtime + iceberg-aws-bundle — écriture des tables Iceberg
#     via le catalogue REST. Iceberg accède au stockage par son propre
#     S3FileIO, qui embarque le SDK AWS v2.
#   * hadoop-aws + aws-java-sdk-bundle — lecture des fichiers CSV en `s3a://`.
#     C'est un système de fichiers Hadoop, totalement distinct du S3FileIO
#     d'Iceberg : sans ces jars, le job écrit dans Iceberg mais ne sait pas
#     lire sa source.
#
# Les jars sont intégrés à l'image plutôt que résolus par `--packages` au
# lancement : le job démarre sans accès réseau et sans latence de résolution
# Maven, et sa reproductibilité ne dépend pas de la disponibilité d'un dépôt.
FROM apache/spark:3.5.6-python3

# Versions alignées : Iceberg 1.9.1 correspond à celle du catalogue REST, et
# aws-java-sdk-bundle 1.12.262 est la version attendue par hadoop-aws 3.3.4,
# lui-même imposé par le client Hadoop embarqué dans Spark 3.5.
ARG ICEBERG_VERSION=1.9.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

USER root

ENV MAVEN_BASE=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# boto3 sert au recensement et à l'archivage des fichiers : ce sont des
# opérations sur des objets, pas sur des données, et les confier à Spark
# reviendrait à mobiliser un moteur distribué pour des appels d'API.
RUN pip3 install --no-cache-dir --no-compile boto3==1.34.162

WORKDIR /app
COPY common/ ./common/
COPY jobs/ ./jobs/

# L'image officielle livre PySpark sous /opt/spark/python sans l'installer dans
# site-packages : c'est `spark-submit` qui l'ajoute au chemin au lancement. Les
# jobs étant exécutés directement par l'interpréteur, le chemin est déclaré ici.
ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

# L'image Spark s'exécute sous l'utilisateur 185 ; le code lui appartient.
RUN chown -R 185:0 /app
USER 185

# Le conteneur reste disponible sans rien exécuter : au Level 1 les jobs sont
# déclenchés par `docker compose exec`, au Level 2 ils le seront par Airflow.
# La grille d'évaluation demande qu'un seul `docker compose up` lance la stack,
# Spark compris.
CMD ["sleep", "infinity"]
