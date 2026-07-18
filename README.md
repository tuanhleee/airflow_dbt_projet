# airflow_dbt_projet

## Architecture du projet

![Architecture du projet](architecture.png)

Le pipeline orchestre d’abord l’ingestion des données vers Databricks, puis exécute les transformations dbt.

Les données passent de la couche Silver vers la couche Gold, où elles sont structurées pour l’analyse à l’aide de dimensions, de modèles dynamiques/incrémentaux et de modèles éphémères basés sur des CTE afin d’éviter la création de tables intermédiaires inutiles.
