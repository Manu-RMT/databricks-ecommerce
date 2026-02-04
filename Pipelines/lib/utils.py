# src/utils.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

sys.path.append("/Workspace/Users/mandu543@gmail.com/databricks-ecommerce/Pipelines/")
# -----------------------------
# Importer la config et transformations
# -----------------------------
from lib.config import *

# -----------------------------
# Fonctions utilitaires
# -----------------------------

def show_schemas(spark: SparkSession):
    """Afficher tous les schemas du catalog 'workspace'"""
    spark.sql("SHOW SCHEMAS IN workspace").show()


def create_schema_if_not_exists(spark: SparkSession, table_name: str):
    """Créer le schema si inexistant à partir d'un nom de table complet"""
    schema_name = table_name.split('.')[1] if '.' in table_name else table_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"Schema {schema_name} created or already exists")


def save_dataframe(df: SparkSession, catalog_zone:str, table_name: str, mode: str = "overwrite"):
    """Sauvegarder un dataframe dans un table"""
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(catalog_zone +"."+ table_name)
    print(f"Table {catalog_zone}.{table_name} saved")


def getDF(spark:SparkSession, catalog_zone:str, table_name: str):
    """Récupérer un dataframe à partir d'une table"""
    return spark.read.table(catalog_zone+"."+table_name)

def getCSVFiles(spark:SparkSession, volume_path: str,schema_data: StructType,separator: str):
    df_csv =  spark.read.csv(volume_path, header=True, schema=schema_data, sep=separator)

    df_csv = df_csv.withColumn("_source_file", col("_metadata.file_path")) \
                .withColumn("_ingested_at", current_timestamp())
    return df_csv

def getFiles(volume_path: str, extension: str):
    # -----------------------------
    # Lister tous les fichiers CSV du Volume
    # -----------------------------
    files = [f.path for f in dbutils.fs.ls(volume_path) if f.name.endswith(extension)]
    print(f"Fichiers {extension} trouvés : {files}")

    # -----------------------------
    # Lire tous les fichiers avec options sécurisées
    # -----------------------------
    # Lire CSV avec première ligne comme header
    # Gérer les champs multi-lignes, guillemets, virgules et espaces inutiles
    dfs = []
    if extension == 'csv':
        for file in files:
                df = spark.read \
                        .option("header", True) \
                        .option("multiLine", True) \
                        .option("escape", '"') \
                        .option("quote", '"') \
                        .option("ignoreLeadingWhiteSpace", True) \
                        .csv(file)
                        
    if extension == 'json':
        df = spark.read.json(files)

    dfs.append(df)
    # -----------------------------
    # Combiner tous les CSV
    # -----------------------------
    df_bronze = dfs[0]


       