# lib/utils.py

import sys
sys.path.append("/Workspace/Users/mandu543@gmail.com/databricks-ecommerce/Pipelines/")

# -----------------------------
# Importer la config et transformations
# -----------------------------
from lib.config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import NumericType
from delta.tables import DeltaTable

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


def normalize_dataframe(df):
   """
   Normalise le DataFrame avec les fonctions importées directement.
   - Strings : Trim, minuscules, conserve alphanumérique et '&'.
   - Autres : Remplacement des NULLs par des valeurs par défaut.
   """
   exprs = []
   for field in df.schema.fields:
       name = field.name
       dtype = field.dataType
       # Utilisation de col_expr pour désigner l'expression de la colonne en cours
       col_expr = col(name)
       # --- TRAITEMENT DES CHAINES DE CARACTÈRES ---
       if isinstance(dtype, StringType):
           # coalesce -> trim -> regexp_replace (sauf alphanum et &) 
           normalized_col = \
               regexp_replace(
                   trim(coalesce(col_expr, lit(""))),
                   r"[^a-zA-Z0-9&:\-, ]", "" # Ajout de l'espace ici pour ne pas coller les mots après le trim
               )
           
           exprs.append(normalized_col.alias(name))

       # --- TRAITEMENT DES NUMÉRIQUES ---
       elif isinstance(dtype, NumericType):
           exprs.append(coalesce(col_expr, lit(0)).alias(name))

       # --- TRAITEMENT DES DATES ---
       elif isinstance(dtype, DateType):
           exprs.append(coalesce(col_expr, to_date(lit("1900-01-01"))).alias(name))

       # --- TRAITEMENT DES TIMESTAMPS ---
       elif isinstance(dtype, TimestampType):
           exprs.append(coalesce(col_expr, to_timestamp(lit("1900-01-01 00:00:00"))).alias(name))

       else:
           # Pour les types non gérés (Booleans, Arrays, etc.)
           exprs.append(col_expr)
   return df.select(*exprs)


def setUppercase(df, columns):
    """Mettre en majuscule les colonnes spécifiées dans la liste 'columns'"""
    for column in columns:
        df = df.withColumn(column, upper(col(column)))
    return df

def setLowercase(df, columns):
    """Mettre en minuscule les colonnes spécifiées dans la liste 'columns'"""
    for column in columns:
        df = df.withColumn(column, lower(col(column)))
    return df

def setPhoneNumber(df,columns):
    """Mettre en forme les numéros de téléphone"""
    for column in columns: 
        df= df.withColumn(column, regexp_replace(col(column), "\\..*",""))
    return df

def cast_columns_spark_types(df, columns_types, date_format="yyyy-MM-dd", timestamp_format="yyyy-MM-dd HH:mm:ss"):
   for column, dtype in columns_types.items():
       if column not in df.columns:
           continue
       if isinstance(dtype, DateType):
           df = df.withColumn(column, to_date(col(column), date_format))
       elif isinstance(dtype, TimestampType):
           df = df.withColumn(column, to_timestamp(col(column), timestamp_format))
       else:
           df = df.withColumn(column, col(column).cast(dtype))
   return df



from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_bronze_to_silver(

    spark,
    source_df,
    target_table,
    table_name,
    reject_path,
    business_key

):

    print(f"--- Début du traitement pour la table : {table_name} ---")

    # -------------------------------------------------------------------------
    # 1. Déduplication optimisée
    # -------------------------------------------------------------------------

    print(f"[1/5] Analyse des doublons sur la source...")

    window_spec = Window.partitionBy(*source_df.columns).orderBy(F.lit(1))
    annotated_df = source_df.withColumn("_row_num", F.row_number().over(window_spec))
    clean_df = annotated_df.filter("_row_num == 1").drop("_row_num")
    duplicates_df = annotated_df.filter("_row_num > 1").drop("_row_num")

    # Vérification des rejets
    if not duplicates_df.rdd.isEmpty():
        duplicate_count = duplicates_df.count()
        print(f"      ⚠️ {duplicate_count} doublons détectés. Export vers : {reject_path}")

        duplicates_df \
            .withColumn("table_name", F.lit(table_name)) \
            .write \
            .mode("append") \
            .option("header", "true") \
            .csv(reject_path)
    else:
        print("      ✅ Aucune ligne en double détectée.")

    # -------------------------------------------------------------------------
    # 2. Préparation du Delta Lake
    # -------------------------------------------------------------------------

    print(f"[2/5] Connexion à la table Silver : {target_table}...")

    delta_table = DeltaTable.forName(spark, target_table)
    join_cond = " AND ".join([f"t.{k} = s.{k}" for k in business_key])

    non_key_cols = [
        c for c in clean_df.columns
        if c not in business_key
        and c != "_ingested_at"
        and c != "date_modification"
    ]

    # -------------------------------------------------------------------------
    # 3. Préparation des conditions de mise à jour
    # -------------------------------------------------------------------------

    print(f"[3/5] Calcul des différences sur {len(non_key_cols)} colonnes métier...")

    update_condition = " OR ".join([f"NOT (t.{c} <=> s.{c})" for c in non_key_cols])

    # -------------------------------------------------------------------------
    # 4. Configuration des dictionnaires de Mapping
    # -------------------------------------------------------------------------

    print(f"[4/5] Préparation du schéma de fusion (Merge)...")

    update_set = {c: f"s.{c}" for c in non_key_cols}
    update_set["date_modification"] = F.current_timestamp()
    insert_set = {c: f"s.{c}" for c in non_key_cols}

    for k in business_key:
        insert_set[k] = f"s.{k}"

    insert_set["date_creation"] = F.col("s._ingested_at")
    insert_set["date_modification"] = F.lit(None)

    # -------------------------------------------------------------------------
    # 5. Exécution du MERGE
    # -------------------------------------------------------------------------

    print(f"[5/5] Exécution du MERGE (Upsert) en cours...")

    delta_table.alias("t") \
        .merge(clean_df.alias("s"), join_cond) \
        .whenMatchedUpdate(
            condition=update_condition, 
            set=update_set
        ) \
        .whenNotMatchedInsert(
            values=insert_set
        ) \
        .execute()

    print(f"✅ Traitement terminé avec succès pour {table_name}.")
    print("-------------------------------------------------------")
 