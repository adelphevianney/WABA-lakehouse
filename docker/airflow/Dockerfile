# Airflow, ordonnanceur de la plateforme (Level 2.1).
#
# Les jobs Spark ne sont pas exécutés dans ce conteneur : Airflow lance pour
# chacun un conteneur `waba/spark` éphémère via le socket Docker. Deux raisons.
#
# Embarquer Spark et une JVM dans l'image Airflow l'alourdirait de près d'un
# gigaoctet pour dupliquer un environnement d'exécution qui existe déjà. Et
# surtout, ce découplage est celui qu'on retrouvera au Level 4 : `DockerOperator`
# devient `SparkKubernetesOperator`, l'ordonnanceur soumettant un travail à un
# exécutant externe plutôt que de le porter lui-même.
FROM apache/airflow:2.10.5-python3.12

# Le fournisseur Docker apporte l'opérateur ; il dialogue avec le démon par le
# SDK Python, sans nécessiter le client en ligne de commande.
RUN pip install --no-cache-dir "apache-airflow-providers-docker==3.14.1"
