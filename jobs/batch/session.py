"""Construction de la session Spark de la plateforme.

Deux accès au stockage cohabitent et répondent à des besoins distincts :

* le catalogue Iceberg écrit via **S3FileIO**, son implémentation propre,
  configurée sous `spark.sql.catalog.iceberg.s3.*` ;
* la lecture des CSV bruts passe par **S3A**, un système de fichiers Hadoop,
  configuré sous `spark.hadoop.fs.s3a.*`.

Ce sont deux piles indépendantes : configurer l'une ne configure pas l'autre, et
une session à laquelle il manque la seconde écrit correctement dans le lakehouse
mais ne sait pas lire sa source.

Le catalogue porte le même nom, `iceberg`, que côté Trino. Les tables se
désignent donc de façon identique dans les deux moteurs — `iceberg.raw.customers`
en PySpark comme en SQL — ce qui évite un décalage de vocabulaire entre le code
et les requêtes de démonstration.
"""

from __future__ import annotations

import os
from typing import Dict

from pyspark.sql import SparkSession

CATALOG = "iceberg"
RAW_NAMESPACE = "raw"


def _credentials() -> Dict[str, str]:
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Identifiants MinIO absents : définir AWS_ACCESS_KEY_ID / "
            "AWS_SECRET_ACCESS_KEY. Voir README.env.example."
        )
    return {"access_key": access_key, "secret_key": secret_key}


def build_session(app_name: str = "waba-batch") -> SparkSession:
    """Session Spark en mode local, configurée pour Iceberg et MinIO."""
    creds = _credentials()
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://lakehouse/")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # --- Catalogue Iceberg, adossé au metastore REST ---------------------
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", rest_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", creds["access_key"])
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", creds["secret_key"])
        .config("spark.sql.defaultCatalog", CATALOG)
        # --- Lecture des CSV bruts en s3a:// ---------------------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", creds["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", creds["secret_key"])
        # MinIO n'expose pas de sous-domaines par bucket.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # --- Réglages d'exécution ---------------------------------------------
        # Les horodatages sont interprétés en UTC quel que soit le fuseau de la
        # machine : huit pays sur trois fuseaux, et un décalage silencieux
        # fausserait tous les agrégats journaliers.
        .config("spark.sql.session.timeZone", "UTC")
        # Le défaut de 200 partitions de mélange produirait, en mode local sur
        # des lots de quelques milliers de lignes, des centaines de fichiers
        # Parquet de quelques kilo-octets par écriture.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.adaptive.enabled", "true")
    )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return session


def ensure_namespace(spark: SparkSession, namespace: str = RAW_NAMESPACE) -> None:
    """Crée le schéma cible s'il n'existe pas encore."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")


def table_name(dataset_table: str, namespace: str = RAW_NAMESPACE) -> str:
    return f"{CATALOG}.{namespace}.{dataset_table}"
